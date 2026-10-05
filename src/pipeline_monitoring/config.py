"""Configuração centralizada."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path


def get_project_root() -> Path:
    """Retorna a raiz do projeto (pasta que contém o pyproject.toml)."""
    current = Path(__file__).resolve()
    for parent in [current.parent, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent
    return current.parents[2]


def setup_logging(level: str | None = None) -> logging.Logger:
    """Configura o logging raiz uma vez e retorna um logger do namespace."""
    resolved = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    logging.basicConfig(
        level=getattr(logging, resolved, logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    return logging.getLogger("pipeline_monitoring")


@dataclass(frozen=True)
class AlertThresholds:
    """Limites padrão dos alertas (podem ser sobrescritos por pipeline no futuro)."""

    max_duration_seconds: float
    max_error_rate: float
    min_rows_expected: int
    max_running_seconds: float = 1800.0


@dataclass(frozen=True)
class Settings:
    """Configurações imutáveis de execução."""

    project_root: Path
    database_path: Path
    thresholds: AlertThresholds
    log_level: str


def load_settings() -> Settings:
    """Carrega as configurações do ambiente, com padrões locais."""
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        pass
    else:
        root = get_project_root()
        env_file = root / ".env"
        if env_file.exists():
            load_dotenv(env_file)

    root = get_project_root()
    db = Path(os.getenv("DATABASE_PATH", "data/monitoring.db"))
    if not db.is_absolute():
        db = root / db
    return Settings(
        project_root=root,
        database_path=db,
        thresholds=AlertThresholds(
            max_duration_seconds=float(os.getenv("MAX_DURATION_SECONDS", "300")),
            max_error_rate=float(os.getenv("MAX_ERROR_RATE", "0.05")),
            min_rows_expected=int(os.getenv("MIN_ROWS_EXPECTED", "1")),
            max_running_seconds=float(os.getenv("MAX_RUNNING_SECONDS", "1800")),
        ),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
