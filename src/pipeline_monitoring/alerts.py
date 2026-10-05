"""Alert rules (pure functions, no I/O — testable in isolation).

Rules (stage 1):
  - pipeline_failed: status == 'failed'
  - duration_exceeded: duration_seconds > max_duration_seconds
  - error_rate_exceeded: rows_failed / max(rows_processed,1) > max_error_rate
  - rows_below_expected: rows_processed < min_rows_expected
"""

from __future__ import annotations

from dataclasses import dataclass

from pipeline_monitoring.config import AlertThresholds


@dataclass(frozen=True)
class Alert:
    """A fired alert."""

    rule: str
    message: str


def evaluate_run(run: dict, thresholds: AlertThresholds) -> list[Alert]:
    """Evaluate one finished run dict against thresholds."""
    alerts: list[Alert] = []
    name = run.get("pipeline_name", "?")
    rid = run.get("run_id", "?")

    if run.get("status") == "failed":
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
