"""Prometheus metrics registry. Centraliza counters/histograms da app.

Em testes, passe um `CollectorRegistry` próprio pra evitar colisão com o registry global.
Em produção, o servidor HTTP `/metrics` (start_metrics_server) usa o registry default.
"""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Histogram


@dataclass(frozen=True)
class Metrics:
    polymarket_requests_total: Counter
    polymarket_request_duration_seconds: Histogram

    # Gamma + CLOB (Plano 2A)
    polymarket_http_request_duration_seconds: Histogram
    polymarket_http_requests_total: Counter

    watcher_iterations_total: Counter
    watcher_trades_inserted_total: Counter
    watcher_iteration_duration_seconds: Histogram

    notifier_messages_total: Counter
    notifier_send_duration_seconds: Histogram


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
    )
