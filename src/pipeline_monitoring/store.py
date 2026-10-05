"""Modelo de run + store SQLite (só stdlib).

Colunas de pipeline_runs:
  run_id, pipeline_name, status, started_at, finished_at,
  duration_seconds, rows_processed, rows_failed, rows_inserted,
  rows_updated, error_message
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running','success','failed')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    duration_seconds REAL,
    rows_processed INTEGER NOT NULL DEFAULT 0,
    rows_failed INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);
"""


@dataclass
class PipelineRun:
    """Uma execução de pipeline (mutável até ser finalizada)."""

    pipeline_name: str
    run_id: str = ""
    status: str = "running"
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    rows_processed: int = 0
    rows_failed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    error_message: str = ""


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path) -> None:
    """Cria a tabela pipeline_runs (idempotente)."""
    with _connect(db_path) as conn:
        conn.executescript(SCHEMA)


def start_run(db_path: Path, pipeline_name: str) -> PipelineRun:
    """Insere uma execução em andamento e a retorna."""
    run = PipelineRun(
        pipeline_name=pipeline_name,
        run_id=uuid.uuid4().hex[:12],
        started_at=datetime.now(timezone.utc).isoformat(),
    )
    with _connect(db_path) as conn:
        conn.execute(
            "INSERT INTO pipeline_runs (run_id, pipeline_name, status, started_at,"
            " rows_processed, rows_failed, rows_inserted, rows_updated)"
            " VALUES (?, ?, 'running', ?, 0, 0, 0, 0)",
            (run.run_id, run.pipeline_name, run.started_at),
        )
        conn.commit()
    logger.info("Run started: %s (%s)", pipeline_name, run.run_id)
    return run


def finish_run(
    db_path: Path,
    run: PipelineRun,
    *,
    status: str,
    rows_processed: int = 0,
    rows_failed: int = 0,
    rows_inserted: int = 0,
    rows_updated: int = 0,
    error_message: str = "",
) -> PipelineRun:
    """Marca uma run como success/failed com métricas. Retorna a run atualizada."""
    if status not in ("success", "failed"):
        raise ValueError(f"Invalid status: {status!r}")
    run.status = status
    run.finished_at = datetime.now(timezone.utc).isoformat()
    start = datetime.fromisoformat(run.started_at)
    end = datetime.fromisoformat(run.finished_at)
    run.duration_seconds = (end - start).total_seconds()
    run.rows_processed = rows_processed
    run.rows_failed = rows_failed
    run.rows_inserted = rows_inserted
    run.rows_updated = rows_updated
    run.error_message = error_message
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE pipeline_runs SET status=?, finished_at=?, duration_seconds=?,"
            " rows_processed=?, rows_failed=?, rows_inserted=?, rows_updated=?,"
            " error_message=? WHERE run_id=?",
            (run.status, run.finished_at, run.duration_seconds, rows_processed,
             rows_failed, rows_inserted, rows_updated, error_message, run.run_id),
        )
        conn.commit()
    logger.info(
        "Run finished: %s (%s) %s in %.2fs (processed=%d failed=%d)",
        run.pipeline_name, run.run_id, status,
        run.duration_seconds, rows_processed, rows_failed,
    )
    return run


def fetch_runs(db_path: Path, pipeline_name: str | None = None) -> list[dict]:
    """Retorna as runs (mais recentes primeiro), opcionalmente filtradas por pipeline."""
    with _connect(db_path) as conn:
        if pipeline_name:
            rows = conn.execute(
                "SELECT * FROM pipeline_runs WHERE pipeline_name=? ORDER BY started_at DESC",
                (pipeline_name,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC"
            ).fetchall()
    return [dict(r) for r in rows]
