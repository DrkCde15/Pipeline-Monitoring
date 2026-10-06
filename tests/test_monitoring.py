"""Testes: ciclo de vida das runs (SQLite) + regras puras de alerta."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline_monitoring.alerts import evaluate_run
from pipeline_monitoring.config import AlertThresholds
from pipeline_monitoring.instrument import monitor, monitored_run
from pipeline_monitoring.runner import simulate_run
from pipeline_monitoring.store import (
    _sanitize_error_message,
    fetch_runs,
    finish_run,
    init_db,
    record_finished_run,
    register_pipeline,
    start_run,
)

THRESHOLDS = AlertThresholds(
    max_duration_seconds=300.0, max_error_rate=0.05, min_rows_expected=10
)


def _ok_run(**overrides) -> dict:
    base = {
        "pipeline_name": "p", "run_id": "r1", "status": "success",
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:00:05+00:00",
        "duration_seconds": 5.0, "rows_processed": 100,
        "rows_failed": 0, "error_message": "",
    }
    base.update(overrides)
    return base


def test_failed_rule_fires() -> None:
    """Status failed sempre alerta."""
    alerts = evaluate_run(_ok_run(status="failed", error_message="boom"), THRESHOLDS)
    assert any(a.rule == "pipeline_failed" for a in alerts)


def test_duration_rule_fires() -> None:
    """Runs lentas alertam acima do limite; rápidas, não."""
    assert any(a.rule == "duration_exceeded"
               for a in evaluate_run(_ok_run(duration_seconds=9999.0), THRESHOLDS))
    assert not any(a.rule == "duration_exceeded"
                   for a in evaluate_run(_ok_run(duration_seconds=1.0), THRESHOLDS))


def test_error_rate_rule_fires() -> None:
    """Taxa de erro alta alerta; runs limpas, não."""
    assert any(a.rule == "error_rate_exceeded"
               for a in evaluate_run(_ok_run(rows_processed=100, rows_failed=50), THRESHOLDS))
    assert not any(a.rule == "error_rate_exceeded"
                   for a in evaluate_run(_ok_run(rows_processed=100, rows_failed=1), THRESHOLDS))


def test_rows_below_expected_fires() -> None:
    """Runs vazias alertam por volume."""
    assert any(a.rule == "rows_below_expected"
               for a in evaluate_run(_ok_run(rows_processed=0), THRESHOLDS))


def _running_run(started_at: str, **overrides) -> dict:
    base = {
        "pipeline_name": "p", "run_id": "r9", "status": "running",
        "started_at": started_at, "finished_at": "",
        "duration_seconds": 0.0, "rows_processed": 0,
        "rows_failed": 0, "error_message": "",
    }
    base.update(overrides)
    return base


def test_stale_running_fires_for_old_run() -> None:
    """Run presa em 'running' além do limite alerta (relógio determinístico)."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    late = start + timedelta(seconds=THRESHOLDS.max_running_seconds + 1)
    alerts = evaluate_run(
        _running_run(start.isoformat()), THRESHOLDS, now=late
    )
    assert [a.rule for a in alerts] == ["stale_running"]


def test_stale_running_silent_for_fresh_run() -> None:
    """Run recém-iniciada não alerta nada — nem dispara regra de run finalizado."""
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    soon = start + timedelta(seconds=10)
    assert evaluate_run(
        _running_run(start.isoformat()), THRESHOLDS, now=soon
    ) == []


def test_stale_running_fires_without_start_time() -> None:
    """Run em execução sem início válido é suspeita: alerta."""
    assert any(a.rule == "stale_running"
               for a in evaluate_run(_running_run(""), THRESHOLDS))
    assert any(a.rule == "stale_running"
               for a in evaluate_run(_running_run("not-a-date"), THRESHOLDS))


def test_unknown_status_raises() -> None:
    """Status ausente ou desconhecido falha alto em vez de avaliar em silêncio."""
    with pytest.raises(ValueError):
        evaluate_run(_ok_run(status="pending"), THRESHOLDS)
    no_status = _ok_run()
    del no_status["status"]
    with pytest.raises(ValueError):
        evaluate_run(no_status, THRESHOLDS)


def test_finished_without_finished_at_raises() -> None:
    """Run 'finalizada' sem finished_at é dado incompleto: falha alto."""
    run = _ok_run()
    del run["finished_at"]
    with pytest.raises(ValueError):
        evaluate_run(run, THRESHOLDS)


def test_store_lifecycle(tmp_path: Path) -> None:
    """start_run -> finish_run persiste métricas recuperáveis via fetch_runs."""
    db = tmp_path / "mon.db"
    init_db(db)
    register_pipeline(db, "demo_pipe")
    run = start_run(db, "demo_pipe")
    finish_run(db, run, status="success", rows_processed=10, rows_inserted=10)
    runs = fetch_runs(db, "demo_pipe")
    assert len(runs) == 1
    assert runs[0]["status"] == "success"
    assert runs[0]["rows_processed"] == 10
    assert runs[0]["duration_seconds"] >= 0.0


def test_start_run_rejects_unknown_pipeline(tmp_path: Path) -> None:
    """Pipeline fora do catálogo falha alto em vez de fragmentar o histórico."""
    db = tmp_path / "mon.db"
    init_db(db)
    with pytest.raises(ValueError):
        start_run(db, "typo_no_catalogo")


def test_register_normalizes_and_rejects_empty(tmp_path: Path) -> None:
    """Registro normaliza espaços e rejeita nome vazio."""
    db = tmp_path / "mon.db"
    init_db(db)
    assert register_pipeline(db, "  demo_pipe  ") == "demo_pipe"
    run = start_run(db, "demo_pipe")  # nome registrado com espaços, usa sem
    assert run.pipeline_name == "demo_pipe"
    with pytest.raises(ValueError):
        register_pipeline(db, "   ")


def test_register_is_idempotent(tmp_path: Path) -> None:
    """Registrar duas vezes não duplica nem falha."""
    db = tmp_path / "mon.db"
    init_db(db)
    register_pipeline(db, "demo_pipe")
    register_pipeline(db, "demo_pipe")
    run = start_run(db, "demo_pipe")
    assert run.pipeline_name == "demo_pipe"


def test_simulate_failed_scenario(tmp_path: Path) -> None:
    """Falha simulada persiste a mensagem de erro e dispara a regra de failed."""
    db = tmp_path / "mon.db"
    init_db(db)
    simulate_run(db, "demo_pipe", scenario="failed", rows_processed=20)
    runs = fetch_runs(db, "demo_pipe")
    assert runs[0]["status"] == "failed"
    assert any(a.rule == "pipeline_failed"
               for a in evaluate_run(runs[0], THRESHOLDS))


def test_sanitize_masks_uri_credentials() -> None:
    """Connection string com usuário/senha não vaza."""
    msg = "connect failed: postgres://admin:s3cr3t@db:5432/app"
    out = _sanitize_error_message(msg)
    assert "s3cr3t" not in out
    assert "postgres://***@db:5432/app" in out


def test_sanitize_masks_key_value_secrets() -> None:
    """Segredos em formato chave=valor são mascarados preservando a chave."""
    out = _sanitize_error_message("auth error: password=supersecret (api_key: xyz)")
    assert "supersecret" not in out and "xyz" not in out
    assert "password=***" in out and "api_key: ***" in out


def test_sanitize_truncates_and_keeps_clean_message() -> None:
    """Mensagem longa é cortada; mensagem limpa passa intacta."""
    long_msg = "x" * 3000
    out = _sanitize_error_message(long_msg)
    assert len(out) <= 2000 + len("… [truncated]")
    assert out.endswith("… [truncated]")
    assert _sanitize_error_message("Simulated connection timeout") == \
        "Simulated connection timeout"


def test_finish_run_persists_sanitized_message(tmp_path: Path) -> None:
    """O que chega ao banco já está sanitizado (ponto único de escrita)."""
    db = tmp_path / "mon.db"
    init_db(db)
    register_pipeline(db, "demo_pipe")
    run = start_run(db, "demo_pipe")
    finish_run(db, run, status="failed",
               error_message="token=abc123 postgres://u:pw@h/db")
    stored = fetch_runs(db, "demo_pipe")[0]["error_message"]
    assert "abc123" not in stored and "u:pw@" not in stored
    assert "token=***" in stored


def test_record_finished_run(tmp_path: Path) -> None:
    """Run finalizada com início/fim explícitos: duração calculada, dict avaliável."""
    from datetime import datetime, timezone

    db = tmp_path / "mon.db"
    init_db(db)
    register_pipeline(db, "airflow_task")
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 0, 1, 30, tzinfo=timezone.utc)
    run = record_finished_run(
        db, "airflow_task", status="success",
        started_at=start.isoformat(), finished_at=end.isoformat(),
        rows_processed=50,
    )
    assert run["duration_seconds"] == 90.0
    assert run["status"] == "success"
    assert evaluate_run(run, THRESHOLDS) == []
    with pytest.raises(ValueError):
        record_finished_run(db, "nao_registrada", status="success",
                            started_at=start.isoformat())


def test_monitored_run_success_collects_metrics(tmp_path: Path) -> None:
    """Bloco ok persiste métricas coletadas no objeto."""
    db = tmp_path / "mon.db"
    init_db(db)
    with monitored_run(db, "etl") as m:
        m.rows_processed = 100
        m.rows_inserted = 100
    stored = fetch_runs(db, "etl")[0]
    assert stored["status"] == "success"
    assert stored["rows_processed"] == 100
    assert m.alerts == []


def test_monitored_run_failure_records_and_reraises(tmp_path: Path) -> None:
    """Exceção no bloco vira run failed (sanitizada) e é relançada."""
    db = tmp_path / "mon.db"
    init_db(db)
    with pytest.raises(RuntimeError, match="boom"):
        with monitored_run(db, "etl") as m:
            m.rows_processed = 10
            raise RuntimeError("boom password=x")
    stored = fetch_runs(db, "etl")[0]
    assert stored["status"] == "failed"
    assert "password=x" not in stored["error_message"]
    assert "password=***" in stored["error_message"]


def test_monitored_run_requires_registration_when_not_auto(tmp_path: Path) -> None:
    """Sem auto_register, pipeline fora do catálogo falha alto."""
    db = tmp_path / "mon.db"
    init_db(db)
    with pytest.raises(ValueError):
        with monitored_run(db, "fantasma", auto_register=False):
            pass


def test_monitor_decorator_captures_dict_and_passes_through(tmp_path: Path) -> None:
    """Decorator captura métricas de dict retornado; int passa intacto."""
    db = tmp_path / "mon.db"
    init_db(db)

    @monitor(db_path=db, pipeline_name="etl_dict")
    def fake_etl() -> dict:
        return {"rows_processed": 7, "rows_inserted": 7}

    @monitor(db_path=db, pipeline_name="etl_int")
    def fake_main() -> int:
        return 0

    assert fake_etl()["rows_processed"] == 7
    assert fake_main() == 0
    by_name = {r["pipeline_name"]: r for r in fetch_runs(db)}
    assert by_name["etl_dict"]["rows_processed"] == 7
    assert by_name["etl_int"]["status"] == "success"
