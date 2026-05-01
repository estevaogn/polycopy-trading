"""Tests for start_metrics_server helper."""

from __future__ import annotations

import socket
from urllib.request import urlopen

from prometheus_client import CollectorRegistry, Counter

from polycopy.infrastructure.observability.http_metrics import start_metrics_server


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_start_metrics_server_serves_metrics_endpoint() -> None:
    registry = CollectorRegistry()
    counter = Counter(
        "polycopy_test_counter",
        "Test counter",
        registry=registry,
    )
    counter.inc()

    port = _free_port()
    server, _thread = start_metrics_server(port, registry=registry)
    try:
        response = urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2.0)
        body = response.read().decode("utf-8")
        assert "polycopy_test_counter" in body
        assert "polycopy_test_counter_total 1.0" in body
    finally:
        server.shutdown()
