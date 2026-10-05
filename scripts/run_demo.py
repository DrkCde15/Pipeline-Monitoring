"""Demo via CLI: simula runs em todos os cenários e avalia os alertas.

Uso:
    python scripts/run_demo.py
"""

from __future__ import annotations

from pipeline_monitoring.alerts import evaluate_run
from pipeline_monitoring.config import AlertThresholds, load_settings, setup_logging
from pipeline_monitoring.runner import simulate_run
from pipeline_monitoring.store import fetch_runs, init_db

logger = setup_logging()


def main() -> int:
    """Simula 4 runs e imprime os alertas disparados. Retorna o exit code."""
    settings = load_settings()
    init_db(settings.database_path)
    # Tight thresholds on purpose so the demo fires alerts deterministically.
    thresholds = AlertThresholds(
        max_duration_seconds=0.3, max_error_rate=0.05, min_rows_expected=10
    )
    scenarios = ["success", "slow", "failed", "empty"]
    for scenario in scenarios:
        simulate_run(settings.database_path, "bronze_ingestion", scenario=scenario)
    runs = fetch_runs(settings.database_path, "bronze_ingestion")[:4]
    fired = 0
    for run in sorted(runs, key=lambda r: r["started_at"]):
        for alert in evaluate_run(run, thresholds):
            print(f"  [ALERT:{alert.rule}] {alert.message}")
            fired += 1
    print(f"Runs: {len(runs)}, alerts fired: {fired}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
