"""Testes: ciclo de vida das runs (SQLite) + regras puras de alerta."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline_monitoring.alerts import evaluate_run
from pipeline_monitoring.config import AlertThresholds
from pipeline_monitoring.runner import simulate_run
from pipeline_monitoring.store import fetch_runs, finish_run, init_db, start_run

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
    run = start_run(db, "demo_pipe")
    finish_run(db, run, status="success", rows_processed=10, rows_inserted=10)
    runs = fetch_runs(db, "demo_pipe")
    assert len(runs) == 1
    assert runs[0]["status"] == "success"
    assert runs[0]["rows_processed"] == 10
    assert runs[0]["duration_seconds"] >= 0.0


def test_simulate_failed_scenario(tmp_path: Path) -> None:
    """Falha simulada persiste a mensagem de erro e dispara a regra de failed."""
    db = tmp_path / "mon.db"
    init_db(db)
    simulate_run(db, "demo_pipe", scenario="failed", rows_processed=20)
    runs = fetch_runs(db, "demo_pipe")
    assert runs[0]["status"] == "failed"
    assert any(a.rule == "pipeline_failed"
               for a in evaluate_run(runs[0], THRESHOLDS))
