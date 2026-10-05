"""Simulated pipeline executor (no real ETL — sleeps + fabricates metrics).

Used to populate pipeline_runs with realistic scenarios:
success, slow success, failed, and empty (below expected rows).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from pipeline_monitoring.store import finish_run, start_run

logger = logging.getLogger(__name__)


def simulate_run(
    db_path: Path,
    pipeline_name: str,
    *,
    scenario: str = "success",
    rows_processed: int = 1000,
) -> dict:
    """Run one simulated execution. Scenario: success|slow|failed|empty."""
    run = start_run(db_path, pipeline_name)
    if scenario == "success":
        time.sleep(0.05)
        finish_run(db_path, run, status="success",
                   rows_processed=rows_processed, rows_inserted=rows_processed)
    elif scenario == "slow":
        time.sleep(0.6)
        finish_run(db_path, run, status="success",
                   rows_processed=rows_processed, rows_inserted=rows_processed)
    elif scenario == "failed":
        time.sleep(0.05)
        finish_run(db_path, run, status="failed",
                   rows_processed=rows_processed, rows_failed=rows_processed,
                   error_message="Simulated connection timeout to source")
    elif scenario == "empty":
        time.sleep(0.02)
        finish_run(db_path, run, status="success", rows_processed=0)
    else:
        raise ValueError(f"Unknown scenario: {scenario!r}")
    return {"run_id": run.run_id, "status": run.status, "scenario": scenario}
