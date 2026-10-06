"""Executor simulado de pipeline (sem ETL real — sleeps + métricas fabricadas).

Usado para popular pipeline_runs com cenários realistas:
success, slow success, failed e empty (abaixo do volume esperado).
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

from pipeline_monitoring.store import finish_run, register_pipeline, start_run

logger = logging.getLogger(__name__)


def simulate_run(
    db_path: Path,
    pipeline_name: str,
    *,
    scenario: str = "success",
    rows_processed: int = 1000,
) -> dict:
    """Executa uma simulação. Cenário: success|slow|failed|empty.

    Helper de demo/teste: registra a pipeline automaticamente.
    Pipelines reais devem usar register_pipeline + start_run,
    onde nome desconhecido falha alto.
    """
    register_pipeline(db_path, pipeline_name)
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
