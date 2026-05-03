# Plano 4 — Web3CLOBExecutor (real on-chain) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. **Cadência: checkpoint humano por task** (NÃO autônomo — Fase 4 mexe em dinheiro real on-chain).

**Goal:** Substituir `DryRunExecutor` (Fase 3 MVP) por `Web3CLOBExecutor` que submete ordens reais no Polymarket CLOB via `py-clob-client`. Strategy Pattern já preparado — `ExecutorAgent` não muda; só DI no `main()` muda.

**Architecture:** Web3CLOBExecutor injeta `KillSwitch` (5 camadas de proteção in-memory) + `py-clob-client` (encapsula EIP-712 signing + submissão off-chain operator + settlement on-chain). `main()` carrega real-mode apenas com triple opt-in (`EXECUTOR_DRY_RUN=false` AND `EXECUTOR_REAL_MODE_CONFIRMED=true` AND `WALLET_PRIVATE_KEY` set). Setup wallet via script CLI manual one-shot.

**Tech Stack:** Python 3.12, `py-clob-client` v0.34.6+ (Polymarket oficial), `web3` v7+ (já instalado), pydantic v2 SecretStr, prometheus_client, pytest + asyncio + monkeypatch.

**Predecessor:** Plano 3 completo (head `8f3960c` + spec Fase 4 `0f6f836`).

**Spec:** `docs/superpowers/specs/2026-05-02-fase-4-web3-executor-design.md`.

---

## File Structure

**Novos arquivos (10):**
- `src/polycopy/infrastructure/execution/kill_switch.py` — `KillSwitch` class (5 camadas + state in-memory).
- `src/polycopy/infrastructure/execution/order_mapper.py` — função pura `Trade → OrderArgs`.
- `src/polycopy/infrastructure/execution/web3_clob_executor.py` — `Web3CLOBExecutor` + factory `build_clob_client()` + `verify_allowance()`.
- `src/polycopy/scripts/__init__.py` (vazio).
- `src/polycopy/scripts/setup_wallet.py` — CLI manual one-shot.
- `tests/unit/infrastructure/test_kill_switch.py`.
- `tests/unit/infrastructure/test_order_mapper.py`.
- `tests/unit/infrastructure/test_web3_clob_executor.py`.
- `tests/unit/scripts/__init__.py` (vazio).
- `tests/unit/scripts/test_setup_wallet.py`.
- `tests/integration/test_polymarket_smoke_executor.py` (opt-in).
- `docs/runbooks/fase-4-first-real-trade.md`.

**Modificados (5):**
- `src/polycopy/domain/events.py` — +10 razões em `FailureReason`.
- `src/polycopy/config.py` — +10 settings (Wallet/RPC/contracts/safety/kill-switches).
- `src/polycopy/infrastructure/observability/metrics.py` — +4 métricas.
- `src/polycopy/agents/executor.py` — `main()` DI condicional + safety gates.
- `tests/unit/domain/test_execution_events.py` — atualizar `test_failure_reason_values`.
- `tests/unit/infrastructure/test_metrics.py` — +4 testes.
- `tests/unit/agents/test_executor.py` — +3 testes (`main()` safety gates).
- `.env.example` — +seção "Fase 4 — DANGER ZONE".
- `pyproject.toml` — +`py-clob-client` dependency.
- `docker-compose.yml` — +10 env vars no service `executor`.

---

## Task 1: Domain — estender `FailureReason` enum (+10 razões)

**Files:**
- Modify: `src/polycopy/domain/events.py`
- Modify: `tests/unit/domain/test_execution_events.py`

**Reviewer:** opcional.

---

- [ ] **Step 1.1: Estender `FailureReason` enum**

LEIA `src/polycopy/domain/events.py` primeiro pra ver o `FailureReason` atual (Fase 3 tem 2 valores).

Substituir o enum por:

```python
class FailureReason(StrEnum):
    """Razões pelas quais Executor falha. Aberto pra extensão (Fase 4)."""

    # Fase 3 (existentes)
    INVALID_TRADE_PARAMS = "invalid_trade_params"
    EXECUTOR_DISABLED = "executor_disabled"

    # Fase 4 — kill-switches (5)
    MANUALLY_PAUSED = "manually_paused"
    DAILY_TRADES_EXCEEDED = "daily_trades_exceeded"
    DAILY_USDC_EXCEEDED = "daily_usdc_exceeded"
    CIRCUIT_BREAKER = "circuit_breaker"
    SIZE_EXCEEDS_EXECUTOR_CAP = "size_exceeds_executor_cap"

    # Fase 4 — falhas on-chain via py-clob-client (5)
    INSUFFICIENT_USDC_BALANCE = "insufficient_usdc_balance"
    INSUFFICIENT_USDC_ALLOWANCE = "insufficient_usdc_allowance"
    CLOB_REJECTED_ORDER = "clob_rejected_order"
    RPC_ERROR = "rpc_error"
    SIGNATURE_ERROR = "signature_error"
```

- [ ] **Step 1.2: Atualizar test `test_failure_reason_values`**

LEIA `tests/unit/domain/test_execution_events.py` primeiro. Localizar `test_failure_reason_values` (Fase 3 tinha 2 valores).

Substituir o teste por:

```python
def test_failure_reason_values() -> None:
    # Fase 3
    assert FailureReason.INVALID_TRADE_PARAMS.value == "invalid_trade_params"
    assert FailureReason.EXECUTOR_DISABLED.value == "executor_disabled"
    # Fase 4 — kill-switches
    assert FailureReason.MANUALLY_PAUSED.value == "manually_paused"
    assert FailureReason.DAILY_TRADES_EXCEEDED.value == "daily_trades_exceeded"
    assert FailureReason.DAILY_USDC_EXCEEDED.value == "daily_usdc_exceeded"
    assert FailureReason.CIRCUIT_BREAKER.value == "circuit_breaker"
    assert FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP.value == "size_exceeds_executor_cap"
    # Fase 4 — on-chain
    assert FailureReason.INSUFFICIENT_USDC_BALANCE.value == "insufficient_usdc_balance"
    assert FailureReason.INSUFFICIENT_USDC_ALLOWANCE.value == "insufficient_usdc_allowance"
    assert FailureReason.CLOB_REJECTED_ORDER.value == "clob_rejected_order"
    assert FailureReason.RPC_ERROR.value == "rpc_error"
    assert FailureReason.SIGNATURE_ERROR.value == "signature_error"
```

- [ ] **Step 1.3: Verificar GREEN + commit**

```bash
uv run pytest tests/unit/domain/test_execution_events.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check src/polycopy/domain/events.py tests/unit/domain/test_execution_events.py
```
Esperado: tudo PASS.

Commit (controller pede confirmação humana antes):
```bash
git add src/polycopy/domain/events.py tests/unit/domain/test_execution_events.py
git commit -m "feat(domain): extend FailureReason with 10 reasons for real-mode (Fase 4)"
```

---

## Task 2: Settings — 10 vars novas + `.env.example` + `pyproject.toml`

**Files:**
- Modify: `src/polycopy/config.py`
- Modify: `.env.example`
- Modify: `pyproject.toml` (add `py-clob-client` dependency)

**Reviewer:** opcional.

---

- [ ] **Step 2.1: Adicionar `py-clob-client` ao `pyproject.toml`**

```bash
uv add py-clob-client
```

Verifica `pyproject.toml` — deve ter `py-clob-client>=0.34.6` em `[project.dependencies]`. Se uv não pinar a versão mínima, editar manualmente pra `>=0.34.6`.

- [ ] **Step 2.2: Adicionar 10 settings em `config.py`**

LEIA `src/polycopy/config.py` primeiro. Atualmente tem 4 executor settings (linhas ~100-103) da Fase 3.

Adicionar `Path` ao import se não estiver:
```python
from pathlib import Path
```

Adicionar bloco no fim da classe `Settings` (após executor_dry_run da Fase 3):

```python
    # --- Fase 4 — Real on-chain execution (DANGER ZONE) ---
    # Wallet (real-mode only — None default fail-fast)
    wallet_private_key: SecretStr | None = Field(None, alias="WALLET_PRIVATE_KEY")

    # Polygon network
    polygon_rpc_url: str = Field(
        "https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY",
        alias="POLYGON_RPC_URL",
    )
    polygon_chain_id: int = Field(137, alias="POLYGON_CHAIN_ID")

    # Polymarket contracts (Polygon mainnet)
    polymarket_exchange_address: str = Field(
        "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
        alias="POLYMARKET_EXCHANGE_ADDRESS",
    )
    polymarket_clob_api_url: str = Field(
        "https://clob.polymarket.com",
        alias="POLYMARKET_CLOB_API_URL",
    )

    # Executor real-mode safety gate (double opt-in)
    executor_real_mode_confirmed: bool = Field(
        False, alias="EXECUTOR_REAL_MODE_CONFIRMED"
    )

    # Approval cap (run setup_wallet script once after funding)
    max_approval_usdc: int = Field(100, alias="MAX_APPROVAL_USDC")

    # Kill-switches (5 camadas)
    executor_max_size_usdc: Decimal = Field(
        Decimal("2"), alias="EXECUTOR_MAX_SIZE_USDC"
    )
    executor_daily_max_usdc: Decimal = Field(
        Decimal("20"), alias="EXECUTOR_DAILY_MAX_USDC"
    )
    executor_daily_max_trades: int = Field(
        10, alias="EXECUTOR_DAILY_MAX_TRADES"
    )
    executor_circuit_breaker_failures: int = Field(
        3, alias="EXECUTOR_CIRCUIT_BREAKER_FAILURES"
    )
    executor_pause_file: Path = Field(
        Path("/tmp/polycopy/executor.pause"), alias="EXECUTOR_PAUSE_FILE"
    )
```

- [ ] **Step 2.3: Atualizar `.env.example`**

LEIA `.env.example` primeiro. Adicionar bloco no final:

```bash
# --- Fase 4 — Real on-chain execution (DANGER ZONE) ---
# Real-mode requires BOTH flags set explicitly:
EXECUTOR_DRY_RUN=true                            # Default safe — set false ONLY after setup_wallet ran
EXECUTOR_REAL_MODE_CONFIRMED=false               # Double opt-in — set true ONLY after testing on small amounts

# Wallet (NEVER commit real key — chmod 600 your .env)
WALLET_PRIVATE_KEY=                               # 0x... (32-byte hex)

# Polygon RPC (Alchemy free tier recommended — get free key at alchemy.com)
POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYGON_CHAIN_ID=137

# Polymarket contracts (Polygon mainnet — leave defaults)
POLYMARKET_EXCHANGE_ADDRESS=0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e
POLYMARKET_CLOB_API_URL=https://clob.polymarket.com

# Approval cap (run `uv run python -m polycopy.scripts.setup_wallet` once after funding)
MAX_APPROVAL_USDC=100

# Kill-switches (defaults conservadores — perda máxima inicial: $2/trade, $20/dia)
EXECUTOR_MAX_SIZE_USDC=2
EXECUTOR_DAILY_MAX_USDC=20
EXECUTOR_DAILY_MAX_TRADES=10
EXECUTOR_CIRCUIT_BREAKER_FAILURES=3
EXECUTOR_PAUSE_FILE=/tmp/polycopy/executor.pause
```

- [ ] **Step 2.4: Verificar mypy + commit**

```bash
uv run mypy src/polycopy
uv run ruff check src/polycopy/config.py
uv run pytest tests/ 2>&1 | tail -5
```
Esperado: tudo PASS. Suite mantém baseline.

Commit:
```bash
git add src/polycopy/config.py .env.example pyproject.toml uv.lock
git commit -m "feat(config): add Fase 4 settings (wallet, RPC, kill-switches) and py-clob-client dep"
```

---

## Task 3: `KillSwitch` class + 12 unit tests

**Files:**
- Create: `src/polycopy/infrastructure/execution/kill_switch.py`
- Create: `tests/unit/infrastructure/test_kill_switch.py`

**Reviewer:** opcional.

---

- [ ] **Step 3.1: Escrever 12 testes unit (RED)**

Create `tests/unit/infrastructure/test_kill_switch.py`:

```python
"""Testes unit do KillSwitch — 5 camadas de proteção in-memory."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from polycopy.domain.events import FailureReason
from polycopy.infrastructure.execution.kill_switch import KillSwitch


def _make_kill_switch(
    *,
    max_size_usdc: Decimal = Decimal("2"),
    daily_max_usdc: Decimal = Decimal("20"),
    daily_max_trades: int = 10,
    circuit_breaker_failures: int = 3,
    pause_file: Path | None = None,
    tmp_path: Path | None = None,
) -> KillSwitch:
    if pause_file is None:
        if tmp_path is None:
            raise RuntimeError("test bug: provide pause_file or tmp_path")
        pause_file = tmp_path / "pause"
    return KillSwitch(
        max_size_usdc=max_size_usdc,
        daily_max_usdc=daily_max_usdc,
        daily_max_trades=daily_max_trades,
        circuit_breaker_failures=circuit_breaker_failures,
        pause_file=pause_file,
    )


def test_check_passes_when_all_clear(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path)
    assert ks.check(Decimal("1")) is None


def test_check_blocks_when_pause_file_exists(tmp_path: Path) -> None:
    pause = tmp_path / "pause"
    pause.touch()
    ks = _make_kill_switch(pause_file=pause)
    assert ks.check(Decimal("1")) == FailureReason.MANUALLY_PAUSED


def test_check_blocks_when_size_exceeds_cap(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, max_size_usdc=Decimal("2"))
    assert ks.check(Decimal("3")) == FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP


def test_check_blocks_when_daily_trades_exceeded(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, daily_max_trades=2)
    ks.record_success(Decimal("1"))
    ks.record_success(Decimal("1"))
    assert ks.check(Decimal("1")) == FailureReason.DAILY_TRADES_EXCEEDED


def test_check_blocks_when_daily_usdc_exceeded(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, daily_max_usdc=Decimal("5"))
    ks.record_success(Decimal("3"))
    # Próximo trade de $3 ultrapassaria 5
    assert ks.check(Decimal("3")) == FailureReason.DAILY_USDC_EXCEEDED


def test_check_blocks_when_circuit_breaker_tripped(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=2)
    ks.record_failure()
    ks.record_failure()
    assert ks.check(Decimal("1")) == FailureReason.CIRCUIT_BREAKER


def test_record_success_resets_circuit_breaker(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=2)
    ks.record_failure()
    ks.record_failure()
    ks.record_success(Decimal("1"))
    # Circuit breaker resetado, próximo check passa
    assert ks.check(Decimal("1")) is None


def test_eviction_window_24h(tmp_path: Path) -> None:
    """Trades > 24h devem sair do contador diário."""
    ks = _make_kill_switch(tmp_path=tmp_path, daily_max_trades=2)
    # Injetar trade antigo (>24h)
    old_ts = datetime.now(tz=UTC) - timedelta(hours=25)
    ks._trades_24h.append((old_ts, Decimal("1")))  # type: ignore[attr-defined]
    # Adicionar 2 trades recentes
    ks.record_success(Decimal("1"))
    ks.record_success(Decimal("1"))
    # Trade antigo já evicted; 2 contam, próximo bloqueia
    assert ks.check(Decimal("1")) == FailureReason.DAILY_TRADES_EXCEEDED


def test_check_order_pause_first(tmp_path: Path) -> None:
    """Ordem das checagens: pause file checado antes de tudo."""
    pause = tmp_path / "pause"
    pause.touch()
    ks = _make_kill_switch(
        pause_file=pause, max_size_usdc=Decimal("2"), circuit_breaker_failures=1
    )
    ks.record_failure()  # circuit breaker tripado
    # Mesmo com circuit breaker, pause file ganha
    assert ks.check(Decimal("100")) == FailureReason.MANUALLY_PAUSED


def test_check_order_circuit_breaker_before_daily(tmp_path: Path) -> None:
    """Ordem: circuit breaker antes de daily trades."""
    ks = _make_kill_switch(
        tmp_path=tmp_path, circuit_breaker_failures=1, daily_max_trades=1
    )
    ks.record_failure()
    ks.record_success(Decimal("1"))  # NOTE: success reseta circuit breaker
    # Reset; agora ks tem 1 trade no dia, daily_max=1
    ks.record_failure()
    # Circuit breaker tripado novamente
    assert ks.check(Decimal("1")) == FailureReason.CIRCUIT_BREAKER


def test_record_success_updates_daily_counter(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path)
    ks.record_success(Decimal("5"))
    ks.record_success(Decimal("3"))
    assert len(ks._trades_24h) == 2  # type: ignore[attr-defined]


def test_record_failure_increments_consecutive(tmp_path: Path) -> None:
    ks = _make_kill_switch(tmp_path=tmp_path, circuit_breaker_failures=5)
    ks.record_failure()
    ks.record_failure()
    assert ks._consecutive_failures == 2  # type: ignore[attr-defined]
```

Run:
```bash
uv run pytest tests/unit/infrastructure/test_kill_switch.py -v 2>&1 | tail -15
```
Esperado: ImportError (KillSwitch não existe).

- [ ] **Step 3.2: Implementar `KillSwitch`**

Create `src/polycopy/infrastructure/execution/kill_switch.py`:

```python
"""KillSwitch: 5 camadas de proteção in-memory pra Web3CLOBExecutor.

Pausa file checado a cada execute (controle externo via filesystem).
Daily caps via deque com janela rolante 24h. Circuit breaker conta
falhas consecutivas, reseta em sucesso.

State é in-memory — restart do agente reseta. Operadores que reiniciam
container "burlam" daily caps. Mitigação aceita pra MVP.
"""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from polycopy.domain.events import FailureReason


class KillSwitch:
    """5 camadas de proteção in-memory.

    Ordem das checagens (fail-fast):
    1. Pause file (controle operacional zero-restart)
    2. Circuit breaker (N falhas consecutivas)
    3. Daily trades cap (rolling 24h)
    4. Daily USDC cap (rolling 24h)
    5. Per-trade size cap

    State:
    - _trades_24h: deque[(timestamp, size_usdc)] com janela rolante 24h
    - _consecutive_failures: int, reset em record_success()
    """

    def __init__(
        self,
        *,
        max_size_usdc: Decimal,
        daily_max_usdc: Decimal,
        daily_max_trades: int,
        circuit_breaker_failures: int,
        pause_file: Path,
    ) -> None:
        self._max_size_usdc = max_size_usdc
        self._daily_max_usdc = daily_max_usdc
        self._daily_max_trades = daily_max_trades
        self._circuit_breaker_failures = circuit_breaker_failures
        self._pause_file = pause_file
        self._trades_24h: deque[tuple[datetime, Decimal]] = deque()
        self._consecutive_failures: int = 0

    def check(self, size_usdc: Decimal) -> FailureReason | None:
        """Retorna razão de bloqueio ou None se passou todas as camadas."""
        self._evict_old_trades()

        if self._pause_file.exists():
            return FailureReason.MANUALLY_PAUSED

        if self._consecutive_failures >= self._circuit_breaker_failures:
            return FailureReason.CIRCUIT_BREAKER

        if len(self._trades_24h) >= self._daily_max_trades:
            return FailureReason.DAILY_TRADES_EXCEEDED

        sum_24h = sum((s for _, s in self._trades_24h), Decimal("0"))
        if sum_24h + size_usdc > self._daily_max_usdc:
            return FailureReason.DAILY_USDC_EXCEEDED

        if size_usdc > self._max_size_usdc:
            return FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP

        return None

    def record_success(self, size_usdc: Decimal) -> None:
        """Registra trade bem-sucedido. Reseta circuit breaker."""
        self._trades_24h.append((datetime.now(tz=UTC), size_usdc))
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        """Incrementa contador de falhas consecutivas (circuit breaker)."""
        self._consecutive_failures += 1

    @property
    def consecutive_failures(self) -> int:
        """Exposto pra métrica Gauge."""
        return self._consecutive_failures

    def _evict_old_trades(self) -> None:
        """Remove trades > 24h da deque (popleft repeatedly)."""
        cutoff = datetime.now(tz=UTC) - timedelta(hours=24)
        while self._trades_24h and self._trades_24h[0][0] < cutoff:
            self._trades_24h.popleft()
```

- [ ] **Step 3.3: Verificar GREEN + ruff + mypy**

```bash
uv run pytest tests/unit/infrastructure/test_kill_switch.py -v 2>&1 | tail -15
uv run mypy src/polycopy
uv run ruff check src/polycopy/infrastructure/execution/kill_switch.py tests/unit/infrastructure/test_kill_switch.py
uv run ruff format --check src/polycopy/infrastructure/execution/kill_switch.py tests/unit/infrastructure/test_kill_switch.py
```
Esperado: 12 PASS.

- [ ] **Step 3.4: Commit**

```bash
git add src/polycopy/infrastructure/execution/kill_switch.py tests/unit/infrastructure/test_kill_switch.py
git commit -m "feat(execution): add KillSwitch with 5 in-memory protection layers"
```

---

## Task 4: `order_mapper.py` + 6 unit tests

**Files:**
- Create: `src/polycopy/infrastructure/execution/order_mapper.py`
- Create: `tests/unit/infrastructure/test_order_mapper.py`

**Reviewer:** opcional.

**Nota crítica sobre `size`:** Polymarket CLOB trabalha em **shares** (unidades de outcome token), não USDC. `shares = usdc_amount / price`. Bug-prone — testes cobrem.

---

- [ ] **Step 4.1: Testes RED**

Create `tests/unit/infrastructure/test_order_mapper.py`:

```python
"""Testes unit do order_mapper — Trade → OrderArgs (py-clob-client format)."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from py_clob_client.order_builder.constants import BUY, SELL

from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.order_mapper import to_order_args


def _trade(*, side: Side = Side.BUY, price: str = "0.5") -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=side,
        price=Price(value=Decimal(price)),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


def test_buy_price_half_one_usdc_yields_two_shares() -> None:
    """BUY @ price 0.5 com $1 USDC → 2 shares (1/0.5)."""
    args = to_order_args(_trade(side=Side.BUY, price="0.5"), Decimal("1"))
    assert args.token_id == "42"
    assert args.price == 0.5
    assert args.size == 2.0
    assert args.side == BUY


def test_buy_price_quarter_one_usdc_yields_four_shares() -> None:
    """BUY @ price 0.25 com $1 USDC → 4 shares (1/0.25)."""
    args = to_order_args(_trade(side=Side.BUY, price="0.25"), Decimal("1"))
    assert args.size == 4.0
    assert args.price == 0.25


def test_sell_price_half_one_usdc_yields_two_shares() -> None:
    """SELL @ price 0.5 com $1 USDC → 2 shares (mesma matemática)."""
    args = to_order_args(_trade(side=Side.SELL, price="0.5"), Decimal("1"))
    assert args.size == 2.0
    assert args.side == SELL


def test_token_id_passed_through() -> None:
    """token_id deve ser preservado intacto."""
    trade = _trade()
    args = to_order_args(trade, Decimal("1"))
    assert args.token_id == trade.token_id.value


def test_side_enum_mapping() -> None:
    """Side.BUY → BUY constant; Side.SELL → SELL constant."""
    args_buy = to_order_args(_trade(side=Side.BUY), Decimal("1"))
    args_sell = to_order_args(_trade(side=Side.SELL), Decimal("1"))
    assert args_buy.side == BUY
    assert args_sell.side == SELL


def test_fractional_size() -> None:
    """price 0.5 + size $0.005 → 0.01 shares."""
    args = to_order_args(_trade(price="0.5"), Decimal("0.005"))
    assert args.size == 0.01
```

Run:
```bash
uv run pytest tests/unit/infrastructure/test_order_mapper.py -v 2>&1 | tail -15
```
Esperado: ImportError.

- [ ] **Step 4.2: Implementar `order_mapper.py`**

Create `src/polycopy/infrastructure/execution/order_mapper.py`:

```python
"""order_mapper: converte Trade (domain) + final_size_usdc em OrderArgs (py-clob-client).

Polymarket CLOB trabalha em SHARES (unidades de outcome token), não USDC.
Conversão crítica: shares = usdc / price. Bug-prone — coberto por testes.
"""

from __future__ import annotations

from decimal import Decimal

from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.constants import BUY, SELL

from polycopy.domain.models import Side, Trade


def to_order_args(trade: Trade, final_size_usdc: Decimal) -> OrderArgs:
    """Mapeia Trade domain + size USDC pra OrderArgs do py-clob-client.

    `shares = final_size_usdc / trade.price.value`.
    py-clob-client espera floats (não Decimal).
    """
    shares = final_size_usdc / trade.price.value
    return OrderArgs(
        token_id=trade.token_id.value,
        price=float(trade.price.value),
        size=float(shares),
        side=BUY if trade.side == Side.BUY else SELL,
    )
```

- [ ] **Step 4.3: GREEN + verificações + commit**

```bash
uv run pytest tests/unit/infrastructure/test_order_mapper.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check ...
```

Commit:
```bash
git add src/polycopy/infrastructure/execution/order_mapper.py tests/unit/infrastructure/test_order_mapper.py
git commit -m "feat(execution): add order_mapper Trade→OrderArgs (USDC→shares)"
```

---

## Task 5: `Web3CLOBExecutor` + factory + `verify_allowance` + 15 unit tests

**Files:**
- Create: `src/polycopy/infrastructure/execution/web3_clob_executor.py`
- Create: `tests/unit/infrastructure/test_web3_clob_executor.py`

**Reviewer:** **OBRIGATÓRIO** (lógica principal real-mode + dependência externa nova `py-clob-client`).

---

- [ ] **Step 5.1: Testes RED (15 testes)**

Create `tests/unit/infrastructure/test_web3_clob_executor.py`:

```python
"""Testes unit do Web3CLOBExecutor — CLOB client mockado, sem rede."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from polycopy.domain.events import ExecutionMode, FailureReason
from polycopy.domain.models import Side, Trade
from polycopy.domain.value_objects import (
    ConditionId,
    Money,
    Price,
    TokenId,
    WalletAddress,
)
from polycopy.infrastructure.execution.kill_switch import KillSwitch
from polycopy.infrastructure.execution.web3_clob_executor import (
    Web3CLOBExecutor,
    _classify_clob_error,
)
from polycopy.infrastructure.observability.metrics import Metrics, make_metrics


def _trade() -> Trade:
    return Trade(
        tx_hash="0x" + "ab" * 32,
        log_index=0,
        wallet=WalletAddress(value="0x" + "1" * 40),
        condition_id=ConditionId(value="0x" + "cd" * 32),
        token_id=TokenId(value="42"),
        side=Side.BUY,
        price=Price(value=Decimal("0.5")),
        size_usdc=Money.from_usdc("10"),
        occurred_at=datetime.now(tz=UTC),
    )


@pytest.fixture
def metrics() -> Metrics:
    return make_metrics(registry=CollectorRegistry())


@pytest.fixture
def kill_switch(tmp_path: Path) -> KillSwitch:
    return KillSwitch(
        max_size_usdc=Decimal("100"),
        daily_max_usdc=Decimal("1000"),
        daily_max_trades=100,
        circuit_breaker_failures=3,
        pause_file=tmp_path / "pause",
    )


def _make_executor(
    *,
    metrics: Metrics,
    kill_switch: KillSwitch,
    clob_client: Any,
    max_size_usdc: Decimal = Decimal("100"),
) -> Web3CLOBExecutor:
    return Web3CLOBExecutor(
        clob_client=clob_client,
        kill_switch=kill_switch,
        max_size_usdc=max_size_usdc,
        metrics=metrics,
    )


# ----- Happy path -----


async def test_execute_happy_path_returns_executed(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """CLOB sucesso → ExecutionResult(mode=REAL, success=True, tx_hash, gas_wei)."""
    clob = MagicMock()
    clob.create_order.return_value = "signed_order"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xabcdef" + "00" * 29,
        "gasUsed": 150000,
    }

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.mode == ExecutionMode.REAL
    assert result.success is True
    assert result.tx_hash == "0xabcdef" + "00" * 29
    assert result.gas_wei == 150000
    assert kill_switch.consecutive_failures == 0


# ----- Kill-switch bloqueio (CLOB nunca chamado) -----


async def test_execute_blocked_by_kill_switch_does_not_call_clob(
    metrics: Metrics, kill_switch: KillSwitch, tmp_path: Path
) -> None:
    """Pause file existe → CLOB nunca é chamado."""
    (tmp_path / "pause").touch()
    clob = MagicMock()
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)

    result = await executor.execute(_trade(), Decimal("1"))

    assert result.success is False
    assert result.failure_reason == FailureReason.MANUALLY_PAUSED
    clob.create_order.assert_not_called()
    clob.post_order.assert_not_called()


async def test_execute_size_exceeds_executor_cap_blocked(
    metrics: Metrics, tmp_path: Path
) -> None:
    """size > max_size_usdc → SIZE_EXCEEDS_EXECUTOR_CAP."""
    ks = KillSwitch(
        max_size_usdc=Decimal("2"),
        daily_max_usdc=Decimal("1000"),
        daily_max_trades=100,
        circuit_breaker_failures=3,
        pause_file=tmp_path / "pause",
    )
    clob = MagicMock()
    executor = Web3CLOBExecutor(
        clob_client=clob, kill_switch=ks, max_size_usdc=Decimal("2"), metrics=metrics
    )

    result = await executor.execute(_trade(), Decimal("3"))

    assert result.failure_reason == FailureReason.SIZE_EXCEEDS_EXECUTOR_CAP
    clob.create_order.assert_not_called()


# ----- CLOB exception classification -----


async def test_clob_rpc_error_returns_rpc_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'rpc' no msg → RPC_ERROR + record_failure()."""
    clob = MagicMock()
    clob.create_order.side_effect = ConnectionError("rpc node down")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.RPC_ERROR
    assert result.error_message == "rpc node down"
    assert kill_switch.consecutive_failures == 1


async def test_clob_signature_error_returns_signature_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'signature' → SIGNATURE_ERROR."""
    clob = MagicMock()
    clob.create_order.side_effect = ValueError("invalid signature")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.SIGNATURE_ERROR


async def test_clob_insufficient_balance_returns_balance_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'balance' → INSUFFICIENT_USDC_BALANCE."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.side_effect = RuntimeError("insufficient balance")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.INSUFFICIENT_USDC_BALANCE


async def test_clob_insufficient_allowance_returns_allowance_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception com 'allowance' → INSUFFICIENT_USDC_ALLOWANCE."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.side_effect = RuntimeError("not enough allowance")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.INSUFFICIENT_USDC_ALLOWANCE


async def test_clob_generic_error_returns_clob_rejected(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Exception genérica → CLOB_REJECTED_ORDER."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.side_effect = RuntimeError("market closed")

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.CLOB_REJECTED_ORDER


async def test_clob_post_returns_success_false(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """response.success=False → CLOB_REJECTED_ORDER."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.return_value = {"success": False, "errorMsg": "rejected"}

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))

    assert result.failure_reason == FailureReason.CLOB_REJECTED_ORDER
    assert result.error_message == "rejected"


# ----- ExecutionResult sempre mode=REAL -----


async def test_execute_always_mode_real(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """Web3CLOBExecutor sempre retorna mode=REAL (nunca DRY_RUN)."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xab",
        "gasUsed": 100,
    }

    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    result = await executor.execute(_trade(), Decimal("1"))
    assert result.mode == ExecutionMode.REAL


# ----- Métricas observadas -----


async def test_metric_kill_switch_blocks_incremented(
    metrics: Metrics, tmp_path: Path
) -> None:
    """kill_switch_blocks_total{reason} incrementa quando bloqueia."""
    pause = tmp_path / "pause"
    pause.touch()
    ks = KillSwitch(
        max_size_usdc=Decimal("100"),
        daily_max_usdc=Decimal("1000"),
        daily_max_trades=100,
        circuit_breaker_failures=3,
        pause_file=pause,
    )
    clob = MagicMock()
    executor = Web3CLOBExecutor(
        clob_client=clob, kill_switch=ks, max_size_usdc=Decimal("100"), metrics=metrics
    )

    await executor.execute(_trade(), Decimal("1"))

    counter = metrics.executor_kill_switch_blocks_total.labels(reason="manually_paused")
    assert counter._value.get() == 1.0


async def test_metric_clob_request_duration_observed_on_success(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """clob_request_duration_seconds{result=success} observa."""
    clob = MagicMock()
    clob.create_order.return_value = "signed"
    clob.post_order.return_value = {
        "success": True,
        "transactionHash": "0xab",
        "gasUsed": 100,
    }
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    await executor.execute(_trade(), Decimal("1"))

    samples = list(metrics.executor_clob_request_duration_seconds.collect())[0].samples
    success_count = next(
        s.value
        for s in samples
        if s.name.endswith("_count") and s.labels.get("result") == "success"
    )
    assert success_count == 1.0


async def test_metric_clob_request_duration_observed_on_error(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """clob_request_duration_seconds{result=error} observa em failure."""
    clob = MagicMock()
    clob.create_order.side_effect = RuntimeError("oops")
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    await executor.execute(_trade(), Decimal("1"))

    samples = list(metrics.executor_clob_request_duration_seconds.collect())[0].samples
    error_count = next(
        s.value
        for s in samples
        if s.name.endswith("_count") and s.labels.get("result") == "error"
    )
    assert error_count == 1.0


async def test_metric_consecutive_failures_gauge_reflects_state(
    metrics: Metrics, kill_switch: KillSwitch
) -> None:
    """consecutive_failures Gauge reflete state do kill_switch."""
    clob = MagicMock()
    clob.create_order.side_effect = RuntimeError("fail1")
    executor = _make_executor(metrics=metrics, kill_switch=kill_switch, clob_client=clob)
    await executor.execute(_trade(), Decimal("1"))

    assert metrics.executor_consecutive_failures._value.get() == 1.0


# ----- _classify_clob_error -----


def test_classify_clob_error_rpc_keyword() -> None:
    assert _classify_clob_error(ConnectionError("rpc timeout")) == FailureReason.RPC_ERROR


def test_classify_clob_error_signature_keyword() -> None:
    assert _classify_clob_error(ValueError("bad signature")) == FailureReason.SIGNATURE_ERROR


def test_classify_clob_error_balance_keyword() -> None:
    assert (
        _classify_clob_error(RuntimeError("not enough balance"))
        == FailureReason.INSUFFICIENT_USDC_BALANCE
    )


def test_classify_clob_error_allowance_keyword() -> None:
    assert (
        _classify_clob_error(RuntimeError("low allowance"))
        == FailureReason.INSUFFICIENT_USDC_ALLOWANCE
    )


def test_classify_clob_error_generic_falls_back_to_rejected() -> None:
    assert (
        _classify_clob_error(RuntimeError("market closed"))
        == FailureReason.CLOB_REJECTED_ORDER
    )
```

Run:
```bash
uv run pytest tests/unit/infrastructure/test_web3_clob_executor.py -v 2>&1 | tail -10
```
Esperado: ImportError (módulo + métricas novas não existem ainda — isso é esperado, métricas são T7).

**Nota importante**: testes assumem que métricas novas (`executor_kill_switch_blocks_total`, `executor_clob_request_duration_seconds`, `executor_consecutive_failures`) já existem em `Metrics`. Se T7 ainda não rodou, essas referências vão falhar.

**Solução**: rodar T7 primeiro? Não — T7 depende de Web3CLOBExecutor existir. Decisão: implementar T5 com métricas inline (cria os Counter/Histogram localmente no construtor), e refatorar em T7 pra usar `Metrics` global.

**ALTERNATIVA SIMPLES**: implementar T5 + T7 juntos (combinar). Mas isso aumenta escopo. Vamos com a abordagem original — T7 vai estender `Metrics` dataclass + atualizar testes T5 ajustando assertions.

**Para T5, simplificar**: assumir métricas existem (testes vão falhar nas linhas de assert metric — implementer fica ciente, fix em T7). Ou usar `pytest.skip` nas assertions de métrica. Decisão final: **T5 cria as métricas localmente como atributos da classe** (`self._kill_switch_blocks_counter = Counter(...)` no `__init__`), depois T7 refatora pra usar `Metrics` injetado. Isso evita acoplamento de ordem.

Pra simplificar, **inverter ordem**: rodar T7 ANTES de T5. Vamos reescrever sequência: T1, T2, **T7 (métricas + main()), T3, T4, T5, T6, T8**. Hmm, mas T7 também atualiza `main()` que precisa T5 existir.

**Solução real**: dividir T7 em duas — T7a (só métricas) ANTES de T5; T7b (main() DI) DEPOIS de T5. Reorganizar plano.

Decisão: **acoplar T5 com T7 num único bloco lógico**. Explicito no plano que T5 adiciona métricas em `metrics.py` antes de implementar `Web3CLOBExecutor`. Atualizar abaixo.

**REORGANIZAÇÃO**: T5 incorpora métricas + executor; T7 fica só com `main()` DI + tests do main(). Vou reescrever inline.

**Substituir T5 do plano por:**

T5: métricas (4 novas) + `Web3CLOBExecutor` + factory + 15 unit tests do executor + 4 testes de métricas.

Conteúdo dos testes de métricas (4 novos em `tests/unit/infrastructure/test_metrics.py`):

```python
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
```

- [ ] **Step 5.2: Adicionar 4 métricas em `metrics.py`**

LEIA primeiro. Adicionar 4 campos no dataclass `Metrics` (após `executor_gas_wei`):

```python
    executor_kill_switch_blocks_total: Counter
    executor_clob_request_duration_seconds: Histogram
    executor_wallet_balance_usdc: Gauge
    executor_consecutive_failures: Gauge
```

Adicionar 4 entries em `make_metrics()`:

```python
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
            "Saldo USDC atual da wallet (atualizado pós-trade success).",
            registry=target,
        ),
        executor_consecutive_failures=Gauge(
            "polycopy_executor_consecutive_failures",
            "Contador atual do circuit breaker (0=saudável; ≥3=trippado).",
            registry=target,
        ),
```

- [ ] **Step 5.3: Adicionar 4 testes de métricas**

Em `tests/unit/infrastructure/test_metrics.py`, adicionar os 4 testes do Step 5.1.

- [ ] **Step 5.4: Implementar `Web3CLOBExecutor`**

Create `src/polycopy/infrastructure/execution/web3_clob_executor.py`:

```python
"""Web3CLOBExecutor: implementação real de OrderExecutor via py-clob-client.

Polygon mainnet only. EOA SIGNATURE_TYPE=0. Usa py-clob-client (oficial
Polymarket) que encapsula EIP-712 signing + submissão pro operator
off-chain + settlement on-chain.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderType

from polycopy.config import Settings
from polycopy.domain.events import ExecutionMode, FailureReason
from polycopy.domain.execution import ExecutionResult
from polycopy.domain.models import Trade
from polycopy.infrastructure.execution.kill_switch import KillSwitch
from polycopy.infrastructure.execution.order_mapper import to_order_args
from polycopy.infrastructure.observability.metrics import Metrics


class Web3CLOBExecutor:
    """Implementação real do OrderExecutor via py-clob-client.

    Strategy injetado no ExecutorAgent quando EXECUTOR_DRY_RUN=false.
    Sempre retorna ExecutionResult(mode=REAL, ...).
    """

    def __init__(
        self,
        *,
        clob_client: ClobClient,
        kill_switch: KillSwitch,
        max_size_usdc: Decimal,
        metrics: Metrics,
    ) -> None:
        self._clob = clob_client
        self._kill_switch = kill_switch
        self._max_size_usdc = max_size_usdc
        self._metrics = metrics

    async def execute(self, trade: Trade, final_size_usdc: Decimal) -> ExecutionResult:
        # 1. Kill-switch (5 camadas, fail-fast)
        block_reason = self._kill_switch.check(final_size_usdc)
        if block_reason is not None:
            self._metrics.executor_kill_switch_blocks_total.labels(
                reason=block_reason.value
            ).inc()
            return ExecutionResult(
                mode=ExecutionMode.REAL,
                success=False,
                failure_reason=block_reason,
                error_message=f"kill_switch blocked: {block_reason.value}",
            )

        # 2. Mapear Trade → OrderArgs
        args = to_order_args(trade, final_size_usdc)

        # 3. Submeter via py-clob-client (sync API → asyncio.to_thread)
        clob_start = time.perf_counter()
        try:
            signed = await asyncio.to_thread(self._clob.create_order, args)
            response: dict[str, Any] = await asyncio.to_thread(
                self._clob.post_order, signed, OrderType.GTC
            )
        except Exception as exc:  # noqa: BLE001 — vira OrderFailed
            self._metrics.executor_clob_request_duration_seconds.labels(
                result="error"
            ).observe(time.perf_counter() - clob_start)
            self._kill_switch.record_failure()
            self._metrics.executor_consecutive_failures.set(
                self._kill_switch.consecutive_failures
            )
            reason = _classify_clob_error(exc)
            return ExecutionResult(
                mode=ExecutionMode.REAL,
                success=False,
                failure_reason=reason,
                error_message=str(exc),
            )

        self._metrics.executor_clob_request_duration_seconds.labels(
            result="success"
        ).observe(time.perf_counter() - clob_start)

        # 4. Verificar response do CLOB
        if not response.get("success", False):
            self._kill_switch.record_failure()
            self._metrics.executor_consecutive_failures.set(
                self._kill_switch.consecutive_failures
            )
            return ExecutionResult(
                mode=ExecutionMode.REAL,
                success=False,
                failure_reason=FailureReason.CLOB_REJECTED_ORDER,
                error_message=str(response.get("errorMsg", "unknown")),
            )

        # 5. Sucesso
        self._kill_switch.record_success(final_size_usdc)
        self._metrics.executor_consecutive_failures.set(0)
        return ExecutionResult(
            mode=ExecutionMode.REAL,
            success=True,
            tx_hash=str(response["transactionHash"]),
            gas_wei=int(response.get("gasUsed", 0)),
        )


def _classify_clob_error(exc: Exception) -> FailureReason:
    """Mapeia exception do py-clob-client pra FailureReason específica.

    Heurística por keyword no mensagem (py-clob-client não tem hierarchy
    rica de exceptions; usa RuntimeError genérico em geral).
    """
    msg = str(exc).lower()
    if "rpc" in msg:
        return FailureReason.RPC_ERROR
    if "signature" in msg:
        return FailureReason.SIGNATURE_ERROR
    if "balance" in msg:
        return FailureReason.INSUFFICIENT_USDC_BALANCE
    if "allowance" in msg:
        return FailureReason.INSUFFICIENT_USDC_ALLOWANCE
    return FailureReason.CLOB_REJECTED_ORDER


def build_clob_client(settings: Settings) -> ClobClient:
    """Factory que monta ClobClient a partir de Settings.

    Usado no main() do executor agent quando real-mode ativo.
    Requer wallet_private_key set (raise se None).
    """
    if settings.wallet_private_key is None:
        raise RuntimeError("WALLET_PRIVATE_KEY required for real-mode")

    pk = settings.wallet_private_key.get_secret_value()
    # Em EOA SIGNATURE_TYPE=0, funder = address derivada da private key.
    # py-clob-client requer funder address explícito.
    from eth_account import Account

    funder_address = Account.from_key(pk).address

    client = ClobClient(
        host=settings.polymarket_clob_api_url,
        key=pk,
        chain_id=settings.polygon_chain_id,
        signature_type=0,  # EOA
        funder=funder_address,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    return client


async def verify_allowance(
    settings: Settings, min_required_usdc: Decimal
) -> None:
    """Verifica que wallet tem allowance >= min_required_usdc pra Exchange.

    Raise RuntimeError se baixa — operador precisa rodar setup_wallet.
    Lê on-chain via web3.py (py-clob-client não expõe método de allowance).
    """
    from web3 import Web3

    if settings.wallet_private_key is None:
        raise RuntimeError("WALLET_PRIVATE_KEY required for verify_allowance")

    pk = settings.wallet_private_key.get_secret_value()
    from eth_account import Account

    wallet_address = Account.from_key(pk).address

    w3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    # USDC contract on Polygon
    usdc_address = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
    usdc_abi = [
        {
            "constant": True,
            "inputs": [
                {"name": "_owner", "type": "address"},
                {"name": "_spender", "type": "address"},
            ],
            "name": "allowance",
            "outputs": [{"name": "", "type": "uint256"}],
            "type": "function",
        }
    ]
    usdc = w3.eth.contract(address=Web3.to_checksum_address(usdc_address), abi=usdc_abi)
    allowance_raw = await asyncio.to_thread(
        usdc.functions.allowance(
            Web3.to_checksum_address(wallet_address),
            Web3.to_checksum_address(settings.polymarket_exchange_address),
        ).call
    )
    allowance_usdc = Decimal(allowance_raw) / Decimal(10**6)  # USDC has 6 decimals
    if allowance_usdc < min_required_usdc:
        raise RuntimeError(
            f"USDC allowance insufficient: have ${allowance_usdc}, "
            f"need >= ${min_required_usdc}. "
            f"Run: uv run python -m polycopy.scripts.setup_wallet"
        )
```

- [ ] **Step 5.5: GREEN + verificações**

```bash
uv run pytest tests/unit/infrastructure/test_web3_clob_executor.py -v 2>&1 | tail -25
uv run pytest tests/unit/infrastructure/test_metrics.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check ...
uv run ruff format --check ...
uv run pytest tests/ 2>&1 | tail -5
```
Esperado: 15 testes do executor + 4 testes de métricas PASS.

- [ ] **Step 5.6: STOP — code reviewer obrigatório + commit**

Reviewer obrigatório (lógica principal real-mode + dependência externa nova). Após reviewer:

```bash
git add src/polycopy/infrastructure/execution/web3_clob_executor.py \
        src/polycopy/infrastructure/observability/metrics.py \
        tests/unit/infrastructure/test_web3_clob_executor.py \
        tests/unit/infrastructure/test_metrics.py
git commit -m "feat(execution): add Web3CLOBExecutor + 4 metrics for real on-chain mode"
```

**Nota retrospectiva (aplicada em commit do fixer):** T5 também adicionou
`web3>=7.16.0` ao `pyproject.toml` — dependência transitiva necessária pra
`verify_allowance` que lê on-chain via Web3.py (py-clob-client não expõe
método de allowance). T2 do plano original só listava `py-clob-client`.
A divergência foi aceita pelo controller + reviewer.

---

## Task 6: `setup_wallet.py` script + 4 unit tests

**Files:**
- Create: `src/polycopy/scripts/__init__.py` (vazio)
- Create: `src/polycopy/scripts/setup_wallet.py`
- Create: `tests/unit/scripts/__init__.py` (vazio)
- Create: `tests/unit/scripts/test_setup_wallet.py`

**Reviewer:** opcional.

---

- [ ] **Step 6.1: Testes RED**

Create `tests/unit/scripts/test_setup_wallet.py`:

```python
"""Testes unit do setup_wallet script — Web3 mockado, sem rede."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from polycopy.scripts.setup_wallet import _approve_usdc, _print_status


def test_print_status_shows_balances_and_allowance(capsys: pytest.CaptureFixture) -> None:
    """Imprime address, MATIC balance, USDC balance, allowance."""
    _print_status(
        wallet_address="0x" + "1" * 40,
        matic_balance=Decimal("5.0"),
        usdc_balance=Decimal("20.0"),
        allowance=Decimal("0"),
        max_approval_usdc=100,
        exchange_address="0xabc",
    )
    captured = capsys.readouterr()
    assert "0x" + "1" * 40 in captured.out
    assert "5.0" in captured.out  # MATIC
    assert "20.0" in captured.out  # USDC
    assert "0" in captured.out  # allowance


def test_approve_usdc_requires_yes_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Confirmação 'no' → não chama approve."""
    monkeypatch.setattr("builtins.input", lambda _: "no")
    web3 = MagicMock()
    usdc_contract = MagicMock()

    result = _approve_usdc(
        web3=web3,
        usdc_contract=usdc_contract,
        wallet_address="0x" + "1" * 40,
        wallet_private_key="0x" + "ab" * 32,
        exchange_address="0xabc",
        max_approval_usdc=100,
    )
    assert result is None  # Não fez approve
    usdc_contract.functions.approve.assert_not_called()


def test_approve_usdc_yes_calls_approve_with_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Confirmação 'yes' → chama approve com cap em micro-USDC."""
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    web3 = MagicMock()
    web3.eth.gas_price = 30 * 10**9
    web3.eth.get_transaction_count.return_value = 0
    web3.eth.send_raw_transaction.return_value = b"\xab" * 32
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

    usdc_contract = MagicMock()
    tx = {"to": "0xusdc", "data": "0x..."}
    usdc_contract.functions.approve.return_value.build_transaction.return_value = tx

    with patch("polycopy.scripts.setup_wallet.Account") as account_mock:
        signed_tx = MagicMock()
        signed_tx.raw_transaction = b"\xcd" * 32
        account_mock.from_key.return_value.sign_transaction.return_value = signed_tx

        result = _approve_usdc(
            web3=web3,
            usdc_contract=usdc_contract,
            wallet_address="0x" + "1" * 40,
            wallet_private_key="0x" + "ab" * 32,
            exchange_address="0xabc",
            max_approval_usdc=100,
        )
        # Verifica approve chamado com 100 USDC * 10^6 micro-USDC
        usdc_contract.functions.approve.assert_called_once_with("0xabc", 100 * 10**6)
        assert result is not None  # tx_hash retornado


def test_approve_usdc_prints_polygonscan_url(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """Após approve sucesso, imprime URL Polygonscan."""
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    web3 = MagicMock()
    web3.eth.gas_price = 30 * 10**9
    web3.eth.get_transaction_count.return_value = 0
    web3.eth.send_raw_transaction.return_value = b"\xab" * 32
    web3.eth.wait_for_transaction_receipt.return_value = {"status": 1}

    usdc_contract = MagicMock()
    tx = {"to": "0xusdc", "data": "0x..."}
    usdc_contract.functions.approve.return_value.build_transaction.return_value = tx

    with patch("polycopy.scripts.setup_wallet.Account") as account_mock:
        signed_tx = MagicMock()
        signed_tx.raw_transaction = b"\xcd" * 32
        account_mock.from_key.return_value.sign_transaction.return_value = signed_tx

        _approve_usdc(
            web3=web3,
            usdc_contract=usdc_contract,
            wallet_address="0x" + "1" * 40,
            wallet_private_key="0x" + "ab" * 32,
            exchange_address="0xabc",
            max_approval_usdc=100,
        )
    captured = capsys.readouterr()
    assert "polygonscan.com" in captured.out
```

Run:
```bash
uv run pytest tests/unit/scripts/test_setup_wallet.py -v 2>&1 | tail -10
```
Esperado: ImportError.

- [ ] **Step 6.2: Implementar `setup_wallet.py`**

Create `src/polycopy/scripts/setup_wallet.py`:

```python
"""setup_wallet: script CLI manual one-shot pra approve USDC pro Exchange.

Roda uma vez após criar EOA + fundar com USDC + MATIC. NÃO roda no agent.

Uso:
    uv run python -m polycopy.scripts.setup_wallet

Comportamento:
    1. Carrega Settings.
    2. Imprime address da wallet, balances MATIC + USDC, allowance atual.
    3. Pergunta confirmação interativa.
    4. Se sim: chama usdc.approve(EXCHANGE, MAX_APPROVAL_USDC * 10^6).
    5. Imprime tx_hash + URL Polygonscan.
"""

from __future__ import annotations

import asyncio
import sys
from decimal import Decimal
from typing import Any

from eth_account import Account
from web3 import Web3

from polycopy.config import Settings


_USDC_ADDRESS_POLYGON = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"
_USDC_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "_spender", "type": "address"},
            {"name": "_value", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [
            {"name": "_owner", "type": "address"},
            {"name": "_spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
]


def _print_status(
    *,
    wallet_address: str,
    matic_balance: Decimal,
    usdc_balance: Decimal,
    allowance: Decimal,
    max_approval_usdc: int,
    exchange_address: str,
) -> None:
    """Imprime status atual da wallet (balances + allowance)."""
    print("=" * 60)
    print("WALLET SETUP — Polymarket CLOB approval")
    print("=" * 60)
    print(f"Wallet address:    {wallet_address}")
    print(f"MATIC balance:     {matic_balance}")
    print(f"USDC balance:      {usdc_balance}")
    print(f"Current allowance: {allowance} USDC")
    print(f"Exchange address:  {exchange_address}")
    print(f"Approval cap:      {max_approval_usdc} USDC")
    print("=" * 60)


def _approve_usdc(
    *,
    web3: Web3,
    usdc_contract: Any,
    wallet_address: str,
    wallet_private_key: str,
    exchange_address: str,
    max_approval_usdc: int,
) -> str | None:
    """Pergunta confirmação interativa e faz approve. Retorna tx_hash hex ou None."""
    confirm = input(f"\nApprove ${max_approval_usdc} USDC for Exchange? (yes/no): ")
    if confirm.lower() != "yes":
        print("Aborted.")
        return None

    nonce = web3.eth.get_transaction_count(Web3.to_checksum_address(wallet_address))
    gas_price = web3.eth.gas_price
    cap_micro = max_approval_usdc * 10**6
    tx = usdc_contract.functions.approve(
        exchange_address, cap_micro
    ).build_transaction(
        {
            "from": Web3.to_checksum_address(wallet_address),
            "nonce": nonce,
            "gas": 100_000,
            "gasPrice": gas_price,
        }
    )
    signed = Account.from_key(wallet_private_key).sign_transaction(tx)
    tx_hash_bytes = web3.eth.send_raw_transaction(signed.raw_transaction)
    tx_hash = tx_hash_bytes.hex()
    print(f"\nTransaction submitted: 0x{tx_hash}")
    print(f"Polygonscan: https://polygonscan.com/tx/0x{tx_hash}")
    print("Waiting for 1 block confirmation...")
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash_bytes, timeout=60)
    if receipt["status"] == 1:
        print("✅ Approval confirmed on-chain.")
    else:
        print("❌ Transaction reverted!")
        return None
    return tx_hash


async def main() -> None:
    """Entrypoint."""
    settings = Settings()  # type: ignore[call-arg]

    if settings.wallet_private_key is None:
        print("ERROR: WALLET_PRIVATE_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    pk = settings.wallet_private_key.get_secret_value()
    wallet_address = Account.from_key(pk).address

    web3 = Web3(Web3.HTTPProvider(settings.polygon_rpc_url))
    if not web3.is_connected():
        print(f"ERROR: cannot connect to {settings.polygon_rpc_url}", file=sys.stderr)
        sys.exit(1)

    matic_balance_wei = web3.eth.get_balance(Web3.to_checksum_address(wallet_address))
    matic_balance = Decimal(matic_balance_wei) / Decimal(10**18)

    usdc_contract = web3.eth.contract(
        address=Web3.to_checksum_address(_USDC_ADDRESS_POLYGON), abi=_USDC_ABI
    )
    usdc_balance_micro = usdc_contract.functions.balanceOf(
        Web3.to_checksum_address(wallet_address)
    ).call()
    usdc_balance = Decimal(usdc_balance_micro) / Decimal(10**6)

    allowance_micro = usdc_contract.functions.allowance(
        Web3.to_checksum_address(wallet_address),
        Web3.to_checksum_address(settings.polymarket_exchange_address),
    ).call()
    allowance = Decimal(allowance_micro) / Decimal(10**6)

    _print_status(
        wallet_address=wallet_address,
        matic_balance=matic_balance,
        usdc_balance=usdc_balance,
        allowance=allowance,
        max_approval_usdc=settings.max_approval_usdc,
        exchange_address=settings.polymarket_exchange_address,
    )

    if matic_balance < Decimal("0.1"):
        print(
            "\n⚠️  WARNING: MATIC balance < 0.1 — may not have enough gas. "
            "Fund wallet with at least $1 worth of MATIC.",
            file=sys.stderr,
        )

    if usdc_balance == 0:
        print(
            "\n⚠️  WARNING: USDC balance is 0 — fund wallet with USDC first "
            "(or skip approval if you'll fund later).",
            file=sys.stderr,
        )

    _approve_usdc(
        web3=web3,
        usdc_contract=usdc_contract,
        wallet_address=wallet_address,
        wallet_private_key=pk,
        exchange_address=settings.polymarket_exchange_address,
        max_approval_usdc=settings.max_approval_usdc,
    )


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6.3: GREEN + verificações + commit**

```bash
uv run pytest tests/unit/scripts/test_setup_wallet.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check ...
```

Commit:
```bash
git add src/polycopy/scripts/__init__.py \
        src/polycopy/scripts/setup_wallet.py \
        tests/unit/scripts/__init__.py \
        tests/unit/scripts/test_setup_wallet.py
git commit -m "feat(scripts): add setup_wallet CLI for USDC approval (one-shot manual)"
```

---

## Task 7: `main()` DI condicional + safety gates + 3 unit tests

**Files:**
- Modify: `src/polycopy/agents/executor.py`
- Modify: `tests/unit/agents/test_executor.py`

**Reviewer:** **OBRIGATÓRIO** (mexe em `main()` que carrega secrets + DI crítico).

---

- [ ] **Step 7.1: Atualizar `main()` em `agents/executor.py`**

LEIA `src/polycopy/agents/executor.py` linhas 255-300 (atual `main()`).

Substituir o bloco `if/else` do executor (linhas ~275-279) por:

```python
    executor: OrderExecutor
    if settings.executor_dry_run:
        from polycopy.infrastructure.execution.dry_run_executor import DryRunExecutor

        executor = DryRunExecutor()
    else:
        # Triple safety gates pra real-mode
        if not settings.executor_real_mode_confirmed:
            raise RuntimeError(
                "Real-mode requires both EXECUTOR_DRY_RUN=false AND "
                "EXECUTOR_REAL_MODE_CONFIRMED=true (double opt-in)"
            )
        if settings.wallet_private_key is None:
            raise RuntimeError("WALLET_PRIVATE_KEY required for real-mode")

        from polycopy.infrastructure.execution.kill_switch import KillSwitch
        from polycopy.infrastructure.execution.web3_clob_executor import (
            Web3CLOBExecutor,
            build_clob_client,
            verify_allowance,
        )

        clob_client = build_clob_client(settings)

        # Verifica allowance suficiente — fail-fast se setup_wallet não rodou
        await verify_allowance(settings, settings.executor_max_size_usdc)

        kill_switch = KillSwitch(
            max_size_usdc=settings.executor_max_size_usdc,
            daily_max_usdc=settings.executor_daily_max_usdc,
            daily_max_trades=settings.executor_daily_max_trades,
            circuit_breaker_failures=settings.executor_circuit_breaker_failures,
            pause_file=settings.executor_pause_file,
        )

        executor = Web3CLOBExecutor(
            clob_client=clob_client,
            kill_switch=kill_switch,
            max_size_usdc=settings.executor_max_size_usdc,
            metrics=metrics,
        )
```

- [ ] **Step 7.2: Adicionar 3 testes em `test_executor.py`**

LEIA `tests/unit/agents/test_executor.py` primeiro pra ver pattern.

Adicionar ao final do arquivo:

```python
async def test_main_raises_when_real_mode_without_confirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Triple safety gate 1: dry_run=false + real_mode_confirmed=false → raise."""
    from polycopy.agents.executor import main

    monkeypatch.setenv("EXECUTOR_DRY_RUN", "false")
    monkeypatch.setenv("EXECUTOR_REAL_MODE_CONFIRMED", "false")
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    with pytest.raises(RuntimeError, match="EXECUTOR_REAL_MODE_CONFIRMED"):
        await main()


async def test_main_raises_when_real_mode_without_private_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Triple safety gate 2: real-mode confirmed sem WALLET_PRIVATE_KEY → raise."""
    from polycopy.agents.executor import main

    monkeypatch.setenv("EXECUTOR_DRY_RUN", "false")
    monkeypatch.setenv("EXECUTOR_REAL_MODE_CONFIRMED", "true")
    # WALLET_PRIVATE_KEY ausente
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    with pytest.raises(RuntimeError, match="WALLET_PRIVATE_KEY"):
        await main()


async def test_main_dry_run_default_does_not_require_wallet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dry-run default: não exige wallet nem real_mode_confirmed.

    Não vamos rodar main() até o fim (precisa NATS+Postgres). Apenas validamos
    que o gate não dispara em dry-run.
    """
    monkeypatch.setenv("EXECUTOR_DRY_RUN", "true")
    monkeypatch.setenv("POSTGRES_USER", "test")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test")
    monkeypatch.setenv("POSTGRES_DB", "test")
    monkeypatch.setenv("NATS_URL", "nats://localhost:4222")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")

    # Settings carrega sem erro — gate de dry_run não toca em real-mode checks.
    from polycopy.config import Settings

    settings = Settings()  # type: ignore[call-arg]
    assert settings.executor_dry_run is True
    assert settings.wallet_private_key is None
    assert settings.executor_real_mode_confirmed is False
    # Esses 3 NÃO bloqueiam dry-run mode.
```

- [ ] **Step 7.3: Verificações + STOP — code reviewer + commit**

```bash
uv run pytest tests/unit/agents/test_executor.py -v 2>&1 | tail -10
uv run mypy src/polycopy
uv run ruff check ...
uv run pytest tests/ 2>&1 | tail -5
```

Reviewer obrigatório.

Commit:
```bash
git add src/polycopy/agents/executor.py tests/unit/agents/test_executor.py
git commit -m "feat(agents): wire Web3CLOBExecutor in main() with triple safety gates"
```

---

## Task 8: Smoke opt-in + runbook humano

**Files:**
- Create: `tests/integration/test_polymarket_smoke_executor.py`
- Create: `docs/runbooks/fase-4-first-real-trade.md`

**Reviewer:** opcional.

---

- [ ] **Step 8.1: Smoke opt-in (read-only — NUNCA submete)**

Create `tests/integration/test_polymarket_smoke_executor.py`:

```python
"""Smoke opt-in contra Polygon mainnet — read-only, nunca submete order.

Rodar com:
    PYTEST_LIVE_POLYGON=1 uv run pytest tests/integration/test_polymarket_smoke_executor.py -v

Exige:
    - WALLET_PRIVATE_KEY configurado em .env
    - POLYGON_RPC_URL Alchemy válido
    - setup_wallet rodado (allowance > 0)

Pula automaticamente se PYTEST_LIVE_POLYGON != "1".
"""

from __future__ import annotations

import os
from decimal import Decimal

import pytest

from polycopy.config import Settings
from polycopy.infrastructure.execution.web3_clob_executor import (
    build_clob_client,
    verify_allowance,
)

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        os.environ.get("PYTEST_LIVE_POLYGON") != "1",
        reason="set PYTEST_LIVE_POLYGON=1 to run live Polygon tests (requires wallet + RPC)",
    ),
]


def test_clob_client_can_authenticate() -> None:
    """Confirma que CLOB API responde a L1 authentication via Alchemy.

    NÃO submete order — apenas verifica auth + lê markets.
    """
    settings = Settings()  # type: ignore[call-arg]
    if settings.wallet_private_key is None:
        pytest.skip("WALLET_PRIVATE_KEY not set — cannot test auth")

    client = build_clob_client(settings)
    # Smoke: ler 1 market — confirma client OK + auth OK
    markets = client.get_markets(next_cursor="")
    assert markets is not None
    assert "data" in markets or "limit_orders" in markets or len(markets) > 0


async def test_wallet_has_funds_and_allowance() -> None:
    """Verifica allowance >= $1 USDC.

    Falha rápido se setup_wallet não rodou.
    """
    settings = Settings()  # type: ignore[call-arg]
    if settings.wallet_private_key is None:
        pytest.skip("WALLET_PRIVATE_KEY not set")

    # Não raise = passou
    await verify_allowance(settings, Decimal("1"))
```

- [ ] **Step 8.2: Runbook humano**

Create `docs/runbooks/fase-4-first-real-trade.md`:

```markdown
# Fase 4 — First Real Trade Runbook

**Audiência:** operador humano (você).
**Quando usar:** após deploy completo da Fase 4 (T1-T7 commitados, container `polycopy-executor` rodando), antes de ativar real-mode pela primeira vez.

## Pré-requisitos

- ✅ `polycopy-executor` container rodando em DRY-RUN (default).
- ✅ Pipeline upstream funcionando: watcher → risk → sizing → executor (logs limpos).
- ✅ Conta Alchemy criada, `POLYGON_RPC_URL` no `.env`.
- ✅ EOA criada (private key + address). Address fundada com:
  - **MATIC**: $5+ (gas)
  - **USDC**: $20-50 (collateral)

## Checklist

### Etapa 1: Setup wallet (one-shot)

```bash
# 1. Confirme .env tem WALLET_PRIVATE_KEY + POLYGON_RPC_URL
grep -E "WALLET_PRIVATE_KEY|POLYGON_RPC_URL" .env

# 2. Rode setup_wallet
uv run python -m polycopy.scripts.setup_wallet
```

**Output esperado:**
- Mostra address, balances MATIC/USDC, allowance atual.
- Pergunta `Approve $100 USDC for Exchange? (yes/no)`.
- Digite `yes`, confirma submissão da tx.
- Imprime tx_hash + URL Polygonscan.
- Aguarda confirmação on-chain (~2s em Polygon).
- Imprime `✅ Approval confirmed on-chain`.

**Se falhar:** verifique gas (MATIC ≥ 0.1) + RPC URL válido.

### Etapa 2: Smoke opt-in (read-only)

```bash
PYTEST_LIVE_POLYGON=1 uv run pytest tests/integration/test_polymarket_smoke_executor.py -v
```

**Esperado:** 2 testes PASS (auth + allowance).

### Etapa 3: Validar pipeline DRY-RUN ainda saudável

```bash
docker compose logs --tail=100 executor | grep executor_decision
```

**Esperado:** logs com `mode=dry_run`, `result=dry_run` ou `failed` (sem real-mode ainda).

Aguarde 1h pra observar o pipeline em DRY-RUN sem erros novos.

### Etapa 4: Checkpoint git

```bash
git status     # confirme working tree limpo
git log -1     # confirme HEAD = T8 da Fase 4
```

### Etapa 5: Ativar real-mode (DOUBLE OPT-IN)

```bash
# Edita .env
sed -i 's/EXECUTOR_DRY_RUN=true/EXECUTOR_DRY_RUN=false/' .env
sed -i 's/EXECUTOR_REAL_MODE_CONFIRMED=false/EXECUTOR_REAL_MODE_CONFIRMED=true/' .env

# Restart executor (apenas)
docker compose restart executor

# Acompanhe logs
docker compose logs -f executor
```

**Esperado nos logs:**
- `agent_started`
- Sem `RuntimeError` (triple safety gates passaram).
- `verify_allowance` passou (sem erro de allowance).

### Etapa 6: Aguardar primeiro trade real

Pode levar minutos a horas (depende de wallets observadas + filtros do Risk + Sizing).

**Quando primeiro trade chegar:**
- Log: `executor_decision mode=real result=executed tx_hash=0x... gas_wei=...`.
- **Verifique a tx no Polygonscan**: `https://polygonscan.com/tx/0x...`.
- Confirme:
  - From = sua wallet address
  - To = Polymarket Exchange (`0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e`)
  - Value reflete trade.
- Verifique saldo USDC pós-trade: deve ter diminuído por `final_size_usdc`.

### Kill-switch operacional (caso algo errado)

Pause execução SEM reiniciar container:

```bash
mkdir -p /tmp/polycopy
touch /tmp/polycopy/executor.pause
```

Próximas tentativas vão retornar `MANUALLY_PAUSED`. Pra reativar:

```bash
rm /tmp/polycopy/executor.pause
```

Pra desativar real-mode completamente:

```bash
sed -i 's/EXECUTOR_DRY_RUN=false/EXECUTOR_DRY_RUN=true/' .env
docker compose restart executor
```

## Métricas a observar

```
http://127.0.0.1:9106/metrics
```

Procure por:
- `polycopy_executor_orders_total{mode="real"}` — devem aparecer após primeiro trade
- `polycopy_executor_kill_switch_blocks_total` — quantos foram bloqueados, por qual razão
- `polycopy_executor_consecutive_failures` — 0 = saudável, ≥3 = circuit breaker tripado
- `polycopy_executor_wallet_balance_usdc` — saldo atual

## Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| `RuntimeError: EXECUTOR_REAL_MODE_CONFIRMED required` | flag não setada | edita `.env` |
| `RuntimeError: WALLET_PRIVATE_KEY required` | chave não no `.env` | adiciona |
| `RuntimeError: USDC allowance insufficient` | setup_wallet não rodou | rode |
| Métrica `consecutive_failures ≥ 3` | RPC ou CLOB instável | investigue logs, pode `restart` container |
| `INSUFFICIENT_USDC_BALANCE` constante | wallet sem fundos | funda mais USDC |
| `INSUFFICIENT_USDC_ALLOWANCE` constante | allowance acabou | rode setup_wallet de novo (incrementa cap) |

## Quando parar

- Após primeiro trade real bem-sucedido + verificado no Polygonscan: você está em produção real-mode.
- Continue observando métricas pelos próximos dias.
- Se precisar aumentar `EXECUTOR_MAX_SIZE_USDC` ou outros caps: edite `.env`, restart, observe.
```

- [ ] **Step 8.3: Commit final do Plano 4**

```bash
git add tests/integration/test_polymarket_smoke_executor.py docs/runbooks/fase-4-first-real-trade.md
git commit -m "test(executor): add live smoke opt-in + runbook for first real trade"
```

---

## Self-Review (autor do plano)

**Spec coverage:**

| Spec § | Coberto em |
|---|---|
| §3.1 +10 razões em FailureReason | T1 |
| §3.1 10 settings novas | T2 |
| §3.1 KillSwitch class | T3 |
| §3.1 order_mapper.py | T4 |
| §3.1 Web3CLOBExecutor + factory + verify_allowance | T5 |
| §3.1 setup_wallet.py | T6 |
| §3.1 4 métricas Prometheus | T5 (combinado) |
| §3.1 main() DI condicional + safety gates | T7 |
| §3.1 smoke opt-in | T8 |
| §3.1 runbook | T8 |
| §3.1 py-clob-client dependency | T2 |
| §5.1/5.2/5.3 fluxos | T5 (executor logic) + T6 (setup_wallet) + T7 (main) |
| §6 tratamento de falhas (12 cenários) | T3 (kill-switches) + T5 (CLOB exceptions) + T7 (startup gates) |
| §7.1 settings flat | T2 |
| §7.2 4 métricas | T5 (combinado) |
| §7.3 logs estruturados | herda da Fase 3 (`executor_decision` event existente em T6 do Plano 3) |
| §8 testes (unit + smoke + manual acceptance) | T3+T4+T5+T6+T7 unit; T8 smoke + runbook |
| §10 open questions | declaradas; nenhuma vira task neste plano |

**Placeholder scan:** sem TBD/TODO/"implement later".

**Type consistency:**
- `KillSwitch.check(size_usdc) -> FailureReason | None` em T3, T5.
- `OrderArgs(token_id, price, size, side)` em T4 (output) + T5 (input pra `clob.create_order`).
- `ExecutionResult(mode, success, tx_hash, gas_wei, failure_reason, error_message)` em T5 (Web3CLOBExecutor.execute return) + Fase 3 unchanged.
- `Web3CLOBExecutor(clob_client, kill_switch, max_size_usdc, metrics)` em T5 (def) + T7 (call no main).
- `build_clob_client(settings) -> ClobClient` em T5 (def) + T7 (call).
- `verify_allowance(settings, min_required_usdc) -> None async` em T5 (def) + T7 (call).
- `Metrics.executor_kill_switch_blocks_total` Counter `{reason}` em T5 (added) + T5 (used).
- `Metrics.executor_clob_request_duration_seconds` Histogram `{result}` em T5.
- `Metrics.executor_wallet_balance_usdc` Gauge em T5.
- `Metrics.executor_consecutive_failures` Gauge em T5.
- `KillSwitch.consecutive_failures` property em T3 (def) + T5 (used).

**Atenção operacional herdada:**
- Container `polycopy-executor` deve ser parado antes de testes E2E (mesmo padrão das Fases anteriores).
- Pré-requisito real-mode: `setup_wallet` rodado (validado por `verify_allowance` no startup).
- Triple opt-in: `EXECUTOR_DRY_RUN=false` + `EXECUTOR_REAL_MODE_CONFIRMED=true` + `WALLET_PRIVATE_KEY` set.

**Code reviewer obrigatório em:** T5 (Web3CLOBExecutor — lógica principal) e T7 (main() — secrets + DI crítico). Outras tasks: opcional.

**Bite-sized check:** cada step é 2-5 minutos. T5 e T6 são as maiores (~250 linhas cada arquivo + ~280 linhas de teste em T5). Implementer faz copy-paste do plano + roda. RED→GREEN→COMMIT padrão respeitado.
