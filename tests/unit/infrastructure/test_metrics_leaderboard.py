from __future__ import annotations

from prometheus_client import CollectorRegistry

from polycopy.infrastructure.observability.metrics import make_metrics


def test_make_metrics_includes_leaderboard_metrics() -> None:
    metrics = make_metrics(registry=CollectorRegistry())
    metrics.leaderboard_requests_total.labels(endpoint="leaderboard", status="200").inc()
    metrics.leaderboard_request_duration_seconds.labels(endpoint="leaderboard").observe(0.1)
