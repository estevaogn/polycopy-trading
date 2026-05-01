"""HTTP server pra expor métricas Prometheus em porta dedicada.

Encapsula `prometheus_client.start_http_server` num helper pra que agentes
não precisem importar `prometheus_client` diretamente. Não bloqueia: roda
em thread daemon. Pra ambientes async, basta chamar no `main()` antes do
loop principal.
"""

from __future__ import annotations

from threading import Thread
from wsgiref.simple_server import WSGIServer

from prometheus_client import REGISTRY, CollectorRegistry, start_http_server


def start_metrics_server(
    port: int,
    *,
    registry: CollectorRegistry | None = None,
) -> tuple[WSGIServer, Thread]:
    """Sobe HTTP server `/metrics` na porta dada, em thread daemon.

    Args:
        port: porta TCP. Conventions: watcher 9101, notifier 9102.
        registry: registry custom (testes). Default = REGISTRY global.

    Retorna `(server, thread)`; agentes podem ignorar (server roda como
    daemon até fim do processo). Testes usam pra cleanup determinístico
    via `server.shutdown()`.
    """
    target = registry if registry is not None else REGISTRY
    return start_http_server(port, registry=target)
