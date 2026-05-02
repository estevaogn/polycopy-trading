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
    assert metrics.polymarket_http_request_duration_seconds is not None
    assert metrics.polymarket_http_requests_total is not None


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


def test_metrics_watcher_iterations_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.watcher_iterations_total.labels(wallet="0xabc", outcome="ok").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_watcher_iterations"]
    assert len(matching) == 1


def test_metrics_watcher_trades_inserted_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.watcher_trades_inserted_total.labels(wallet="0xabc").inc(3)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_watcher_trades_inserted"]
    assert len(matching) == 1


def test_metrics_watcher_iteration_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.watcher_iteration_duration_seconds.labels(wallet="0xabc").observe(0.05)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_watcher_iteration_duration_seconds"]
    assert matching


def test_metrics_notifier_messages_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.notifier_messages_total.labels(outcome="sent").inc()
    metrics.notifier_messages_total.labels(outcome="telegram_error").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_notifier_messages"]
    assert len(matching) == 1


def test_metrics_notifier_send_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.notifier_send_duration_seconds.observe(0.123)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_notifier_send_duration_seconds"]
    assert matching


def test_metrics_marketdata_sync_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.marketdata_sync_total.labels(result="ok").inc()
    metrics.marketdata_sync_total.labels(result="fail").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_marketdata_sync"]
    assert len(matching) == 1


def test_metrics_marketdata_sync_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.marketdata_sync_duration_seconds.observe(0.42)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_marketdata_sync_duration_seconds"]
    assert matching


def test_metrics_marketdata_markets_tracked_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.marketdata_markets_tracked.set(42)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_marketdata_markets_tracked"]
    assert matching


def test_metrics_risk_decisions_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.risk_decisions_total.labels(result="approved", reason="none").inc()
    metrics.risk_decisions_total.labels(result="rejected", reason="size_exceeded").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_risk_decisions"]
    assert len(matching) == 1


def test_metrics_risk_decision_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.risk_decision_duration_seconds.observe(0.15)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_risk_decision_duration_seconds"]
    assert matching


def test_metrics_market_cache_hits_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.market_cache_hits_total.labels(result="hit_fresh").inc()
    metrics.market_cache_hits_total.labels(result="hit_stale").inc()
    metrics.market_cache_hits_total.labels(result="miss").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_market_cache_hits"]
    assert len(matching) == 1


def test_metrics_risk_lazy_fetch_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.risk_lazy_fetch_total.labels(result="success").inc()
    metrics.risk_lazy_fetch_total.labels(result="fail").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_risk_lazy_fetch"]
    assert len(matching) == 1
