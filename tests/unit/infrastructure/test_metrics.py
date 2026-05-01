"""Tests for prometheus metrics registry."""

from __future__ import annotations

from prometheus_client import CollectorRegistry

from polycopy.infrastructure.observability.metrics import (
    Metrics,
    make_metrics,
)


def test_make_metrics_returns_metrics_instance() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    assert isinstance(metrics, Metrics)


def test_metrics_polymarket_request_counter_labels() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.polymarket_requests_total.labels(endpoint="activity", status="200").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_polymarket_requests"]
    assert len(matching) == 1


def test_metrics_polymarket_latency_histogram_records() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.polymarket_request_duration_seconds.labels(endpoint="activity").observe(0.123)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_polymarket_request_duration_seconds"]
    assert matching, "histogram não foi registrado"
