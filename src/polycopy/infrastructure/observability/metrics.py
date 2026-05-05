"""Prometheus metrics registry. Centraliza counters/histograms da app.

Em testes, passe um `CollectorRegistry` próprio pra evitar colisão com o registry global.
Em produção, o servidor HTTP `/metrics` (start_metrics_server) usa o registry default.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram


@dataclass(frozen=True)
class Metrics:
    polymarket_requests_total: Counter
    polymarket_request_duration_seconds: Histogram
    polymarket_rows_skipped_total: Counter

    # Gamma + CLOB (Plano 2A)
    polymarket_http_request_duration_seconds: Histogram
    polymarket_http_requests_total: Counter

    watcher_iterations_total: Counter
    watcher_trades_inserted_total: Counter
    watcher_iteration_duration_seconds: Histogram

    notifier_messages_total: Counter
    notifier_send_duration_seconds: Histogram

    marketdata_sync_total: Counter
    marketdata_sync_duration_seconds: Histogram
    marketdata_markets_tracked: Gauge

    risk_decisions_total: Counter
    risk_decision_duration_seconds: Histogram
    market_cache_hits_total: Counter
    risk_lazy_fetch_total: Counter

    sizing_decisions_total: Counter
    sizing_decision_duration_seconds: Histogram
    sizing_size_ratio_observed: Histogram

    executor_orders_total: Counter
    executor_decision_duration_seconds: Histogram
    executor_gas_wei: Histogram
    executor_kill_switch_blocks_total: Counter
    executor_clob_request_duration_seconds: Histogram
    executor_wallet_balance_usdc: Gauge
    executor_consecutive_failures: Gauge

    # Resolver agent (Plano 5A)
    resolver_sync_total: Counter
    resolver_sync_duration_seconds: Histogram
    resolver_resolutions_detected_total: Counter
    resolver_unresolved_pending: Gauge

    # Executor expected price (Plano 5B)
    executor_expected_price_unavailable_total: Counter

    # Hypothetical PnL gauges (Plano 5C)
    hypothetical_pnl_total_usdc: Gauge
    hypothetical_pnl_24h_usdc: Gauge
    hypothetical_winrate: Gauge
    hypothetical_trades_resolved: Gauge
    hypothetical_trades_pending: Gauge

    # Leaderboard discovery (Fase 6)
    leaderboard_requests_total: Counter
    leaderboard_request_duration_seconds: Histogram


def make_metrics(registry: CollectorRegistry | None = None) -> Metrics:
    target = registry if registry is not None else REGISTRY
    return Metrics(
        polymarket_requests_total=Counter(
            "polycopy_polymarket_requests",
            "Total HTTP requests para Polymarket Data API",
            labelnames=["endpoint", "status"],
            registry=target,
        ),
        polymarket_request_duration_seconds=Histogram(
            "polycopy_polymarket_request_duration_seconds",
            "Latência de requests pra Polymarket Data API",
            labelnames=["endpoint"],
            registry=target,
        ),
        polymarket_rows_skipped_total=Counter(
            "polycopy_polymarket_rows_skipped",
            "Trades rows da Polymarket Data API descartados por payload malformado.",
            labelnames=["reason"],
            registry=target,
        ),
        polymarket_http_request_duration_seconds=Histogram(
            "polycopy_polymarket_http_request_duration_seconds",
            "Latência HTTP por client Polymarket (gamma|clob).",
            labelnames=["client", "endpoint", "status"],
            registry=target,
        ),
        polymarket_http_requests_total=Counter(
            "polycopy_polymarket_http_requests",
            "Total de requests HTTP por client Polymarket (gamma|clob).",
            labelnames=["client", "endpoint", "status"],
            registry=target,
        ),
        watcher_iterations_total=Counter(
            "polycopy_watcher_iterations",
            "Iterações de polling do watcher por wallet",
            labelnames=["wallet", "outcome"],
            registry=target,
        ),
        watcher_trades_inserted_total=Counter(
            "polycopy_watcher_trades_inserted",
            "Trades novos inseridos pelo watcher (após dedup PK)",
            labelnames=["wallet"],
            registry=target,
        ),
        watcher_iteration_duration_seconds=Histogram(
            "polycopy_watcher_iteration_duration_seconds",
            "Duração de uma iteração de polling por wallet",
            labelnames=["wallet"],
            registry=target,
        ),
        notifier_messages_total=Counter(
            "polycopy_notifier_messages",
            "Mensagens processadas pelo notifier",
            labelnames=["outcome"],
            registry=target,
        ),
        notifier_send_duration_seconds=Histogram(
            "polycopy_notifier_send_duration_seconds",
            "Duração do envio de uma mensagem (incluindo Telegram API)",
            registry=target,
        ),
        marketdata_sync_total=Counter(
            "polycopy_marketdata_sync",
            "Iterações de sync do MarketDataAgent.",
            labelnames=["result"],
            registry=target,
        ),
        marketdata_sync_duration_seconds=Histogram(
            "polycopy_marketdata_sync_duration_seconds",
            "Duração de uma iteração de sync.",
            registry=target,
        ),
        marketdata_markets_tracked=Gauge(
            "polycopy_marketdata_markets_tracked",
            "Número de mercados sincronizados na última iteração.",
            registry=target,
        ),
        risk_decisions_total=Counter(
            "polycopy_risk_decisions",
            "Decisões do RiskAgent.",
            labelnames=["result", "reason"],
            registry=target,
        ),
        risk_decision_duration_seconds=Histogram(
            "polycopy_risk_decision_duration_seconds",
            "Duração end-to-end de uma decisão.",
            registry=target,
        ),
        market_cache_hits_total=Counter(
            "polycopy_market_cache_hits",
            "Resultado de leitura do MarketRepository (Plano 2B consumer).",
            labelnames=["result"],
            registry=target,
        ),
        risk_lazy_fetch_total=Counter(
            "polycopy_risk_lazy_fetch",
            "Lazy fetch via Gamma quando cache stale/miss.",
            labelnames=["result"],
            registry=target,
        ),
        sizing_decisions_total=Counter(
            "polycopy_sizing_decisions",
            "Decisões do SizingAgent.",
            labelnames=["result", "reason"],
            registry=target,
        ),
        sizing_decision_duration_seconds=Histogram(
            "polycopy_sizing_decision_duration_seconds",
            "Duração end-to-end de uma decisão de sizing.",
            registry=target,
        ),
        sizing_size_ratio_observed=Histogram(
            "polycopy_sizing_size_ratio_observed",
            "Razão final_size / original_size observada por decisão sized.",
            registry=target,
        ),
        executor_orders_total=Counter(
            "polycopy_executor_orders",
            "Decisões do ExecutorAgent.",
            labelnames=["result", "mode", "reason"],
            registry=target,
        ),
        executor_decision_duration_seconds=Histogram(
            "polycopy_executor_decision_duration_seconds",
            "Duração end-to-end de uma decisão de execução.",
            registry=target,
        ),
        executor_gas_wei=Histogram(
            "polycopy_executor_gas_wei",
            "Gas usado em wei (real-mode com result=executed; vazio em dry_run).",
            buckets=(1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12),
            registry=target,
        ),
        executor_kill_switch_blocks_total=Counter(
            "polycopy_executor_kill_switch_blocks",
            "Quantas vezes cada camada de kill-switch bloqueou.",
            labelnames=["reason"],
            registry=target,
        ),
        executor_clob_request_duration_seconds=Histogram(
            "polycopy_executor_clob_request_duration_seconds",
            "Latência da chamada ao CLOB API.",
            labelnames=["result"],
            registry=target,
        ),
        executor_wallet_balance_usdc=Gauge(
            "polycopy_executor_wallet_balance_usdc",
            "Saldo USDC atual da wallet — atualizado por main() no startup. "
            "Snapshot inicial; em produção real, monitorar Polygonscan diretamente.",
            registry=target,
        ),
        executor_consecutive_failures=Gauge(
            "polycopy_executor_consecutive_failures",
            "Contador atual do circuit breaker (0=saudável; ≥3=trippado).",
            registry=target,
        ),
        resolver_sync_total=Counter(
            "polycopy_resolver_sync",
            "Iterações de sync do ResolverAgent.",
            labelnames=["result"],
            registry=target,
        ),
        resolver_sync_duration_seconds=Histogram(
            "polycopy_resolver_sync_duration_seconds",
            "Duração end-to-end de uma iteração de sync.",
            registry=target,
        ),
        resolver_resolutions_detected_total=Counter(
            "polycopy_resolver_resolutions_detected",
            "Resoluções gravadas em market_resolutions.",
            labelnames=["outcome"],
            registry=target,
        ),
        resolver_unresolved_pending=Gauge(
            "polycopy_resolver_unresolved_pending",
            "Backlog atual de condition_ids unresolved (atualizado a cada loop).",
            registry=target,
        ),
        executor_expected_price_unavailable_total=Counter(
            "polycopy_executor_expected_price_unavailable",
            "Trades onde expected_avg_price não pôde ser calculado.",
            labelnames=["reason"],
            registry=target,
        ),
        hypothetical_pnl_total_usdc=Gauge(
            "polycopy_hypothetical_pnl_total_usdc",
            "PnL hipotético acumulado em USDC (todos os trades resolvidos).",
            registry=target,
        ),
        hypothetical_pnl_24h_usdc=Gauge(
            "polycopy_hypothetical_pnl_24h_usdc",
            "PnL hipotético dos últimos 24h em USDC.",
            registry=target,
        ),
        hypothetical_winrate=Gauge(
            "polycopy_hypothetical_winrate",
            "Taxa de vitória dos trades resolvidos (0..1, exclui invalid).",
            registry=target,
        ),
        hypothetical_trades_resolved=Gauge(
            "polycopy_hypothetical_trades_resolved",
            "Quantidade de trades resolvidos (win+lose+invalid).",
            registry=target,
        ),
        hypothetical_trades_pending=Gauge(
            "polycopy_hypothetical_trades_pending",
            "Quantidade de trades aguardando resolução de mercado.",
            registry=target,
        ),
        leaderboard_requests_total=Counter(
            "polycopy_leaderboard_requests",
            "Total HTTP requests para Polymarket leaderboard endpoint.",
            labelnames=["endpoint", "status"],
            registry=target,
        ),
        leaderboard_request_duration_seconds=Histogram(
            "polycopy_leaderboard_request_duration_seconds",
            "Latência do endpoint /v1/leaderboard.",
            labelnames=["endpoint"],
            registry=target,
        ),
    )
