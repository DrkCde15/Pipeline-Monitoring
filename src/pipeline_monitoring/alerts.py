"""Regras de alerta (funções puras, sem I/O — testáveis isoladamente).

Regras (etapa 1):
  - pipeline_failed: status == 'failed'
  - duration_exceeded: duration_seconds > max_duration_seconds
  - error_rate_exceeded: rows_failed / max(rows_processed,1) > max_error_rate
  - rows_below_expected: rows_processed < min_rows_expected
  - stale_running: status == 'running' com started_at mais antigo que
    max_running_seconds (run provavelmente morreu sem finish_run)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pipeline_monitoring.config import AlertThresholds


@dataclass(frozen=True)
class Alert:
    """Um alerta disparado."""

    rule: str
    message: str


def _parse_started_at(value: object) -> datetime | None:
    """Interpreta um timestamp ISO de início; None quando ausente ou malformado."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def evaluate_run(
    run: dict, thresholds: AlertThresholds, *, now: datetime | None = None
) -> list[Alert]:
    """Avalia um dict de run contra os limites.

    Só runs finalizados passam pelas regras de resultado; runs ainda
    'running' são verificadas apenas quanto a stale, pois suas métricas
    estão parciais e gerariam falsos positivos. `now` é injetável para
    que os testes não dependam do relógio.

    Levanta ValueError para status ausente/desconhecido ou run finalizada
    sem finished_at — avaliar dado incompleto como finalizado seria um
    erro silencioso.
    """
    alerts: list[Alert] = []
    name = run.get("pipeline_name", "?")
    rid = run.get("run_id", "?")

    status = run.get("status")
    if status == "running":
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        started = _parse_started_at(run.get("started_at"))
        if started is None:
            alerts.append(Alert(
                "stale_running",
                f"{name} ({rid}) still running with unknown start time",
            ))
        else:
            age = (current - started).total_seconds()
            if age > thresholds.max_running_seconds:
                alerts.append(Alert(
                    "stale_running",
                    f"{name} ({rid}) running for {age:.0f}s"
                    f" > {thresholds.max_running_seconds:.0f}s without finish",
                ))
        return alerts
    if status not in ("success", "failed"):
        raise ValueError(
            f"Unknown run status: {status!r} — evaluate finished runs only"
        )
    if not run.get("finished_at"):
        raise ValueError(
            f"Run {rid} has status {status!r} but no finished_at"
            " — evaluate finished runs only"
        )

    if status == "failed":
        alerts.append(Alert(
            "pipeline_failed",
            f"{name} ({rid}) failed: {run.get('error_message') or 'no message'}",
        ))
    duration = float(run.get("duration_seconds") or 0.0)
    if duration > thresholds.max_duration_seconds:
        alerts.append(Alert(
            "duration_exceeded",
            f"{name} ({rid}) took {duration:.1f}s > {thresholds.max_duration_seconds:.0f}s",
        ))
    processed = int(run.get("rows_processed") or 0)
    failed = int(run.get("rows_failed") or 0)
    error_rate = failed / max(processed, 1)
    if processed > 0 and error_rate > thresholds.max_error_rate:
        alerts.append(Alert(
            "error_rate_exceeded",
            f"{name} ({rid}) error rate {error_rate:.1%} > {thresholds.max_error_rate:.1%}",
        ))
    if processed < thresholds.min_rows_expected:
        alerts.append(Alert(
            "rows_below_expected",
            f"{name} ({rid}) processed {processed} < expected {thresholds.min_rows_expected}",
        ))
    return alerts
