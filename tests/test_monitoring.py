"""Tests: run lifecycle (SQLite) + pure alert rules."""

from __future__ import annotations

from pathlib import Path

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
        "duration_seconds": 5.0, "rows_processed": 100,
        "rows_failed": 0, "error_message": "",
    }
    base.update(overrides)
    return base


def test_failed_rule_fires() -> None:
    """Failed status always alerts."""
    alerts = evaluate_run(_ok_run(status="failed", error_message="boom"), THRESHOLDS)
    assert any(a.rule == "pipeline_failed" for a in alerts)


def test_duration_rule_fires() -> None:
    """Slow runs alert when over the limit; fast ones do not."""
    assert any(a.rule == "duration_exceeded"
               for a in evaluate_run(_ok_run(duration_seconds=9999.0), THRESHOLDS))
    assert not any(a.rule == "duration_exceeded"
                   for a in evaluate_run(_ok_run(duration_seconds=1.0), THRESHOLDS))


def test_error_rate_rule_fires() -> None:
    """High error rates alert; clean runs do not."""
    assert any(a.rule == "error_rate_exceeded"
               for a in evaluate_run(_ok_run(rows_processed=100, rows_failed=50), THRESHOLDS))
    assert not any(a.rule == "error_rate_exceeded"
                   for a in evaluate_run(_ok_run(rows_processed=100, rows_failed=1), THRESHOLDS))


def test_rows_below_expected_fires() -> None:
    """Empty runs alert on volume."""
    assert any(a.rule == "rows_below_expected"
               for a in evaluate_run(_ok_run(rows_processed=0), THRESHOLDS))


def test_store_lifecycle(tmp_path: Path) -> None:
    """start_run -> finish_run persists metrics retrievable via fetch_runs."""
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
    """Simulated failure persists error message and fires the failed rule."""
    db = tmp_path / "mon.db"
    init_db(db)
    simulate_run(db, "demo_pipe", scenario="failed", rows_processed=20)
    runs = fetch_runs(db, "demo_pipe")
    assert runs[0]["status"] == "failed"
    assert any(a.rule == "pipeline_failed"
               for a in evaluate_run(runs[0], THRESHOLDS))
