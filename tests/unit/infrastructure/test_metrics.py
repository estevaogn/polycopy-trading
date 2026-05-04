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


def test_metrics_sizing_decisions_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.sizing_decisions_total.labels(result="sized", reason="none").inc()
    metrics.sizing_decisions_total.labels(result="skipped", reason="below_min_size").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_decisions"]
    assert len(matching) == 1


def test_metrics_sizing_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.sizing_decision_duration_seconds.observe(0.05)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_decision_duration_seconds"]
    assert matching


def test_metrics_sizing_size_ratio_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.sizing_size_ratio_observed.observe(0.1)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_sizing_size_ratio_observed"]
    assert matching


def test_metrics_executor_orders_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_orders_total.labels(result="dry_run", mode="dry_run", reason="none").inc()
    metrics.executor_orders_total.labels(
        result="failed", mode="real", reason="executor_disabled"
    ).inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_orders"]
    assert len(matching) == 1


def test_metrics_executor_decision_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_decision_duration_seconds.observe(0.05)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_decision_duration_seconds"]
    assert matching


def test_metrics_executor_gas_wei_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_gas_wei.observe(1e8)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_gas_wei"]
    assert matching


def test_metrics_executor_kill_switch_blocks_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_kill_switch_blocks_total.labels(reason="manually_paused").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_kill_switch_blocks"]
    assert len(matching) == 1


def test_metrics_executor_clob_request_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_clob_request_duration_seconds.labels(result="success").observe(0.1)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_clob_request_duration_seconds"]
    assert matching


def test_metrics_executor_wallet_balance_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_wallet_balance_usdc.set(50.0)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_wallet_balance_usdc"]
    assert matching


def test_metrics_executor_consecutive_failures_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_consecutive_failures.set(2.0)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_consecutive_failures"]
    assert matching


def test_metrics_resolver_sync_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_sync_total.labels(result="ok").inc()
    metrics.resolver_sync_total.labels(result="fail").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_sync"]
    assert len(matching) == 1


def test_metrics_resolver_sync_duration_histogram() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_sync_duration_seconds.observe(1.5)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_sync_duration_seconds"]
    assert matching


def test_metrics_resolver_resolutions_detected_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_resolutions_detected_total.labels(outcome="yes").inc()
    metrics.resolver_resolutions_detected_total.labels(outcome="no").inc()
    metrics.resolver_resolutions_detected_total.labels(outcome="invalid").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_resolutions_detected"]
    assert len(matching) == 1


def test_metrics_resolver_unresolved_pending_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.resolver_unresolved_pending.set(42)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_resolver_unresolved_pending"]
    assert matching


def test_metrics_executor_expected_price_unavailable_counter() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.executor_expected_price_unavailable_total.labels(reason="empty_book").inc()
    metrics.executor_expected_price_unavailable_total.labels(reason="insufficient_volume").inc()
    metrics.executor_expected_price_unavailable_total.labels(reason="fetch_failed").inc()
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_executor_expected_price_unavailable"]
    assert len(matching) == 1


def test_metrics_hypothetical_pnl_total_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_pnl_total_usdc.set(42.5)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_pnl_total_usdc"]
    assert matching


def test_metrics_hypothetical_pnl_24h_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_pnl_24h_usdc.set(-12.3)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_pnl_24h_usdc"]
    assert matching


def test_metrics_hypothetical_winrate_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_winrate.set(0.61)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_winrate"]
    assert matching


def test_metrics_hypothetical_trades_resolved_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_trades_resolved.set(35)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_trades_resolved"]
    assert matching


def test_metrics_hypothetical_trades_pending_gauge() -> None:
    registry = CollectorRegistry()
    metrics = make_metrics(registry=registry)
    metrics.hypothetical_trades_pending.set(7)
    samples = list(registry.collect())
    matching = [m for m in samples if m.name == "polycopy_hypothetical_trades_pending"]
    assert matching
