"""Instrumentação de pipelines reais (context manager + decorator).

Uso no script ETL (vale dentro e fora do Airflow):

    with monitored_run(db, "bronze") as run:
        ...  # ETL real aqui
        run.rows_processed = n
        run.rows_inserted = n

Na saída: success com as métricas coletadas; exceção: failed com a
mensagem sanitizada, e a exceção é relançada. Alertas avaliados e
logados, disponíveis em `run.alerts` após o bloco.
"""

from __future__ import annotations

import functools
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator

from pipeline_monitoring.alerts import Alert, evaluate_run
from pipeline_monitoring.config import AlertThresholds, load_settings
from pipeline_monitoring.store import (
    finish_run,
    register_pipeline,
    start_run,
)

logger = logging.getLogger(__name__)


@dataclass
class RunMetrics:
    """Métricas coletadas durante a run + alertas avaliados na saída."""

    rows_processed: int = 0
    rows_failed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    alerts: list[Alert] = field(default_factory=list)


@contextmanager
def monitored_run(
    db_path: Path | str,
    pipeline_name: str,
    *,
    auto_register: bool = True,
    thresholds: AlertThresholds | None = None,
) -> Iterator[RunMetrics]:
    """Registra a run do bloco: success com métricas, failed com erro sanitizado."""
    db = Path(db_path)
    if auto_register:
        register_pipeline(db, pipeline_name)
    run = start_run(db, pipeline_name)
    metrics = RunMetrics()
    try:
        yield metrics
    except Exception as exc:
        finish_run(
            db, run, status="failed",
            rows_processed=metrics.rows_processed,
            rows_failed=metrics.rows_failed,
            rows_inserted=metrics.rows_inserted,
            rows_updated=metrics.rows_updated,
            error_message=str(exc),
        )
        raise
    finish_run(
        db, run, status="success",
        rows_processed=metrics.rows_processed,
        rows_failed=metrics.rows_failed,
        rows_inserted=metrics.rows_inserted,
        rows_updated=metrics.rows_updated,
    )
    limits = thresholds or load_settings().thresholds
    stored = {
        "run_id": run.run_id,
        "pipeline_name": run.pipeline_name,
        "status": run.status,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_seconds": run.duration_seconds,
        "rows_processed": run.rows_processed,
        "rows_failed": run.rows_failed,
        "error_message": run.error_message,
    }
    metrics.alerts = evaluate_run(stored, limits)
    for alert in metrics.alerts:
        logger.warning("[monitoring:%s] %s", alert.rule, alert.message)


def monitor(
    _fn: Callable[..., Any] | None = None,
    *,
    db_path: Path | str | None = None,
    pipeline_name: str | None = None,
    auto_register: bool = True,
) -> Callable[..., Any]:
    """Decora um main() de script: registra a run; métricas via dict retornado.

    Se a função retornar um dict com chaves rows_* elas são capturadas;
    qualquer outro retorno passa intacto. Exceção vira run failed.
    Sem argumentos usa o nome da função e o DATABASE_PATH do ambiente.
    """
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        name = pipeline_name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            db = Path(db_path) if db_path else load_settings().database_path
            with monitored_run(db, name, auto_register=auto_register) as metrics:
                result = fn(*args, **kwargs)
                if isinstance(result, dict):
                    for key in ("rows_processed", "rows_failed",
                                "rows_inserted", "rows_updated"):
                        if key in result:
                            setattr(metrics, key, int(result[key]))
                return result

        return wrapper

    return deco(_fn) if _fn is not None else deco
