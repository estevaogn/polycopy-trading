# Fase 6 — Multi-Wallet Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a one-shot CLI that queries Polymarket's public leaderboard endpoint and emits `wallets_candidates.yaml` + `discover_wallets_report.md` for manual review and promotion to `wallets_seed.yaml`.

**Architecture:** Port + adapter (`PolymarketLeaderboardPort` + `PolymarketLeaderboardClient`), pure-domain functions for filter/format (`domain/discovery.py`), thin CLI script (`scripts/discover_wallets.py`). Pure functions are tested without mocks; adapter is tested with `httpx.MockTransport`; one live integration test is gated by `PYTEST_LIVE_POLYMARKET=1`. No DB, no JetStream, no container.

**Tech Stack:** Python 3.12, httpx, tenacity, pydantic, pyyaml, prometheus_client, argparse, pytest. Mypy strict. Pre-commit (ruff + mypy + commitizen) blocks each commit.

**Spec:** `docs/superpowers/specs/2026-05-04-fase-6-multi-wallet-discovery-design.md`

---

## File Structure

**Create:**
- `src/polycopy/domain/discovery.py` — enums (`TimePeriod`, `Category`, `OrderBy`), dataclasses (`LeaderboardEntry`, `CandidateWallet`), pure functions (`derive_label`, `filter_and_rank`, `render_candidates_yaml`, `render_report_md`).
- `src/polycopy/ports/polymarket_leaderboard.py` — `PolymarketLeaderboardPort` Protocol.
- `src/polycopy/infrastructure/polymarket/leaderboard_client.py` — `PolymarketLeaderboardClient` (httpx + tenacity + métricas).
- `src/polycopy/scripts/discover_wallets.py` — CLI thin (argparse + composition).
- `tests/unit/domain/test_discovery.py`
- `tests/unit/infrastructure/test_leaderboard_client.py`
- `tests/unit/scripts/test_discover_wallets_cli.py`
- `tests/integration/test_leaderboard_live.py`

**Modify:**
- `src/polycopy/infrastructure/observability/metrics.py:14-65,68-272` — add 2 fields to `Metrics` dataclass + factory in `make_metrics()`.

**Reuse (read-only):**
- `src/polycopy/config.py:64` — `Settings.polymarket_base_url`.
- `src/polycopy/domain/value_objects.py:75-88` — `WalletAddress`.
- `src/polycopy/infrastructure/wallets_seed.py` — `load_wallets_seed`.
- `src/polycopy/infrastructure/observability/logging.py` — `configure_logging`.

---

## Conventions used throughout

- **Commit style:** Conventional commits enforced by commitizen pre-commit hook. Pattern: `<type>(<scope>): <subject>`. Types used: `feat`, `test`, `refactor`. Scopes: `domain`, `ports`, `infra`, `scripts`, `observability`.
- **Pre-commit:** Each `git commit` runs ruff-check (with `--fix`), ruff-format, mypy strict on `src/`, commitizen on the message. If any fails, fix the underlying issue and re-stage; never use `--no-verify`.
- **TDD:** For each task — write failing test → run, see fail → implement → run, see pass → commit. Edge cases get their own test functions in the same file.
- **Test commands:** Always `uv run pytest <path> -v` from repo root.
- **`from __future__ import annotations`** at top of every new module.

---

## Task 1: Domain enums and dataclasses

**Files:**
- Create: `src/polycopy/domain/discovery.py`
- Test: `tests/unit/domain/test_discovery.py`

- [ ] **Step 1.1: Write the failing test for enums and dataclasses**

Create `tests/unit/domain/test_discovery.py`:

```python
from __future__ import annotations

from decimal import Decimal

import pytest

from polycopy.domain.discovery import (
    Category,
    CandidateWallet,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)
from polycopy.domain.value_objects import WalletAddress


class TestEnums:
    def test_time_period_values(self) -> None:
        assert TimePeriod.DAY.value == "DAY"
        assert TimePeriod.WEEK.value == "WEEK"
        assert TimePeriod.MONTH.value == "MONTH"
        assert TimePeriod.ALL.value == "ALL"

    def test_category_overall(self) -> None:
        assert Category.OVERALL.value == "OVERALL"

    def test_category_has_all_ten(self) -> None:
        names = {c.name for c in Category}
        assert names == {
            "OVERALL", "POLITICS", "SPORTS", "CRYPTO", "CULTURE",
            "MENTIONS", "WEATHER", "ECONOMICS", "TECH", "FINANCE",
        }

    def test_order_by_values(self) -> None:
        assert OrderBy.PNL.value == "PNL"
        assert OrderBy.VOL.value == "VOL"


class TestLeaderboardEntry:
    def test_minimum_fields(self) -> None:
        entry = LeaderboardEntry(
            rank=1,
            address=WalletAddress(value="0x" + "a" * 40),
            user_name="alice",
            volume_usdc=Decimal("1000"),
            pnl_usdc=Decimal("100"),
            verified_badge=True,
        )
        assert entry.rank == 1
        assert entry.user_name == "alice"

    def test_user_name_can_be_none(self) -> None:
        entry = LeaderboardEntry(
            rank=2,
            address=WalletAddress(value="0x" + "b" * 40),
            user_name=None,
            volume_usdc=Decimal("0"),
            pnl_usdc=Decimal("0"),
            verified_badge=False,
        )
        assert entry.user_name is None

    def test_frozen(self) -> None:
        entry = LeaderboardEntry(
            rank=1,
            address=WalletAddress(value="0x" + "c" * 40),
            user_name="x",
            volume_usdc=Decimal("0"),
            pnl_usdc=Decimal("0"),
            verified_badge=False,
        )
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
            entry.rank = 99  # type: ignore[misc]


class TestCandidateWallet:
    def test_minimum_fields(self) -> None:
        cand = CandidateWallet(
            address=WalletAddress(value="0x" + "d" * 40),
            label="alice",
            rank=1,
            volume_usdc=Decimal("1000"),
            pnl_usdc=Decimal("100"),
            verified_badge=True,
        )
        assert cand.label == "alice"
```

- [ ] **Step 1.2: Run test to verify it fails**

Run: `uv run pytest tests/unit/domain/test_discovery.py -v`
Expected: `ModuleNotFoundError: No module named 'polycopy.domain.discovery'`

- [ ] **Step 1.3: Create `src/polycopy/domain/discovery.py`**

```python
"""Domain types and pure functions for wallet discovery (Fase 6).

Types correspond to Polymarket leaderboard API:
https://data-api.polymarket.com/v1/leaderboard
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from polycopy.domain.value_objects import WalletAddress


class TimePeriod(str, Enum):
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    ALL = "ALL"


class Category(str, Enum):
    OVERALL = "OVERALL"
    POLITICS = "POLITICS"
    SPORTS = "SPORTS"
    CRYPTO = "CRYPTO"
    CULTURE = "CULTURE"
    MENTIONS = "MENTIONS"
    WEATHER = "WEATHER"
    ECONOMICS = "ECONOMICS"
    TECH = "TECH"
    FINANCE = "FINANCE"


class OrderBy(str, Enum):
    PNL = "PNL"
    VOL = "VOL"


@dataclass(frozen=True)
class LeaderboardEntry:
    rank: int
    address: WalletAddress
    user_name: str | None
    volume_usdc: Decimal
    pnl_usdc: Decimal
    verified_badge: bool


@dataclass(frozen=True)
class CandidateWallet:
    address: WalletAddress
    label: str
    rank: int
    volume_usdc: Decimal
    pnl_usdc: Decimal
    verified_badge: bool
```

- [ ] **Step 1.4: Run test to verify it passes**

Run: `uv run pytest tests/unit/domain/test_discovery.py -v`
Expected: PASS (all 7 tests).

- [ ] **Step 1.5: Commit**

```bash
git add src/polycopy/domain/discovery.py tests/unit/domain/test_discovery.py
git commit -m "feat(domain): add discovery enums and dataclasses for Fase 6"
```

---

## Task 2: `derive_label` pure function

**Files:**
- Modify: `src/polycopy/domain/discovery.py` (append function)
- Modify: `tests/unit/domain/test_discovery.py` (append test class)

- [ ] **Step 2.1: Append failing test class to `tests/unit/domain/test_discovery.py`**

Append at the end of the file:

```python
from polycopy.domain.discovery import derive_label


class TestDeriveLabel:
    def _entry(self, addr_hex: str = "a" * 40, user_name: str | None = None) -> LeaderboardEntry:
        return LeaderboardEntry(
            rank=1,
            address=WalletAddress(value="0x" + addr_hex),
            user_name=user_name,
            volume_usdc=Decimal("0"),
            pnl_usdc=Decimal("0"),
            verified_badge=False,
        )

    def test_user_name_present(self) -> None:
        assert derive_label(self._entry(user_name="alice")) == "alice"

    def test_user_name_trimmed(self) -> None:
        assert derive_label(self._entry(user_name="  bob  ")) == "bob"

    def test_user_name_whitespace_replaced_with_underscore(self) -> None:
        assert derive_label(self._entry(user_name="alice smith")) == "alice_smith"

    def test_user_name_internal_multiple_whitespace_collapsed(self) -> None:
        assert derive_label(self._entry(user_name="a   b\tc")) == "a_b_c"

    def test_user_name_non_printable_dropped(self) -> None:
        assert derive_label(self._entry(user_name="al\x00ice")) == "alice"

    def test_user_name_max_32_chars(self) -> None:
        long_name = "x" * 100
        assert len(derive_label(self._entry(user_name=long_name))) == 32

    def test_user_name_none_falls_back_to_address_prefix(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name=None))
        assert result == "0xcafef00d…"

    def test_user_name_empty_string_falls_back(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name=""))
        assert result == "0xcafef00d…"

    def test_user_name_only_whitespace_falls_back(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name="   "))
        assert result == "0xcafef00d…"

    def test_user_name_only_non_printable_falls_back(self) -> None:
        result = derive_label(self._entry(addr_hex="cafef00d" + "0" * 32, user_name="\x00\x01"))
        assert result == "0xcafef00d…"
```

- [ ] **Step 2.2: Run tests to see them fail**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestDeriveLabel -v`
Expected: ImportError: cannot import name 'derive_label'.

- [ ] **Step 2.3: Append `derive_label` to `src/polycopy/domain/discovery.py`**

Add at the end of the file (after `CandidateWallet`):

```python
import re

_LABEL_MAX_LEN = 32
_FALLBACK_ADDR_PREFIX_CHARS = 10  # "0x" + 8 hex


def derive_label(entry: LeaderboardEntry) -> str:
    """Return a sanitized label for a leaderboard entry.

    Sanitization rules:
    - trim leading/trailing whitespace
    - replace runs of whitespace with single '_'
    - drop non-printable characters
    - cap at 32 chars
    - fall back to '0x<8-hex>…' when user_name is empty after sanitization
    """
    raw = (entry.user_name or "").strip()
    collapsed = re.sub(r"\s+", "_", raw)
    printable = "".join(ch for ch in collapsed if ch.isprintable())
    if not printable:
        return f"{entry.address.value[:_FALLBACK_ADDR_PREFIX_CHARS]}…"
    return printable[:_LABEL_MAX_LEN]
```

Move the `import re` to the top of the file (after `from __future__ import annotations`):

```python
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from polycopy.domain.value_objects import WalletAddress
```

- [ ] **Step 2.4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestDeriveLabel -v`
Expected: PASS (10 tests).

- [ ] **Step 2.5: Commit**

```bash
git add src/polycopy/domain/discovery.py tests/unit/domain/test_discovery.py
git commit -m "feat(domain): add derive_label with sanitization and fallback"
```

---

## Task 3: `filter_and_rank` pure function

**Files:**
- Modify: `src/polycopy/domain/discovery.py` (append function)
- Modify: `tests/unit/domain/test_discovery.py` (append test class)

- [ ] **Step 3.1: Append failing tests**

Append to `tests/unit/domain/test_discovery.py`:

```python
from polycopy.domain.discovery import filter_and_rank


class TestFilterAndRank:
    def _entry(
        self, *, rank: int, addr_hex: str, vol: str, pnl: str, name: str = "user",
    ) -> LeaderboardEntry:
        return LeaderboardEntry(
            rank=rank,
            address=WalletAddress(value="0x" + addr_hex),
            user_name=name,
            volume_usdc=Decimal(vol),
            pnl_usdc=Decimal(pnl),
            verified_badge=False,
        )

    def test_keeps_order_from_input(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500"),
            self._entry(rank=2, addr_hex="b" * 40, vol="20000", pnl="400"),
            self._entry(rank=3, addr_hex="c" * 40, vol="20000", pnl="300"),
        ]
        result = filter_and_rank(
            entries, min_volume_usdc=Decimal("0"), exclude=set(), top_n=10,
        )
        assert [c.rank for c in result] == [1, 2, 3]

    def test_excludes_seed_addresses(self) -> None:
        excluded = WalletAddress(value="0x" + "a" * 40)
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500"),
            self._entry(rank=2, addr_hex="b" * 40, vol="20000", pnl="400"),
        ]
        result = filter_and_rank(
            entries, min_volume_usdc=Decimal("0"), exclude={excluded}, top_n=10,
        )
        assert len(result) == 1
        assert result[0].address.value == "0x" + "b" * 40

    def test_filters_by_min_volume(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="100", pnl="500"),
            self._entry(rank=2, addr_hex="b" * 40, vol="20000", pnl="400"),
        ]
        result = filter_and_rank(
            entries, min_volume_usdc=Decimal("1000"), exclude=set(), top_n=10,
        )
        assert len(result) == 1
        assert result[0].address.value == "0x" + "b" * 40

    def test_top_n_caps_output(self) -> None:
        entries = [
            self._entry(rank=i, addr_hex=f"{i:040x}", vol="20000", pnl="100")
            for i in range(1, 6)
        ]
        result = filter_and_rank(
            entries, min_volume_usdc=Decimal("0"), exclude=set(), top_n=2,
        )
        assert len(result) == 2

    def test_dedups_by_address(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500"),
            self._entry(rank=2, addr_hex="a" * 40, vol="20000", pnl="500"),
        ]
        result = filter_and_rank(
            entries, min_volume_usdc=Decimal("0"), exclude=set(), top_n=10,
        )
        assert len(result) == 1

    def test_empty_input(self) -> None:
        result = filter_and_rank(
            [], min_volume_usdc=Decimal("0"), exclude=set(), top_n=10,
        )
        assert result == []

    def test_label_derived_in_output(self) -> None:
        entries = [
            self._entry(rank=1, addr_hex="a" * 40, vol="20000", pnl="500", name="alice"),
        ]
        result = filter_and_rank(
            entries, min_volume_usdc=Decimal("0"), exclude=set(), top_n=10,
        )
        assert result[0].label == "alice"
```

- [ ] **Step 3.2: Run tests to see them fail**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestFilterAndRank -v`
Expected: ImportError.

- [ ] **Step 3.3: Append `filter_and_rank`**

Append to `src/polycopy/domain/discovery.py`:

```python
def filter_and_rank(
    entries: list[LeaderboardEntry],
    *,
    min_volume_usdc: Decimal,
    exclude: set[WalletAddress],
    top_n: int,
) -> list[CandidateWallet]:
    """Filter, dedup, and convert entries to candidates.

    - Drops entries whose address is in `exclude`.
    - Drops entries whose `volume_usdc < min_volume_usdc`.
    - Dedups by address (first occurrence wins).
    - Preserves input order (caller should pass entries already sorted by PNL desc).
    - Truncates result to `top_n`.
    """
    seen: set[WalletAddress] = set()
    out: list[CandidateWallet] = []
    for e in entries:
        if e.address in exclude:
            continue
        if e.volume_usdc < min_volume_usdc:
            continue
        if e.address in seen:
            continue
        seen.add(e.address)
        out.append(
            CandidateWallet(
                address=e.address,
                label=derive_label(e),
                rank=e.rank,
                volume_usdc=e.volume_usdc,
                pnl_usdc=e.pnl_usdc,
                verified_badge=e.verified_badge,
            )
        )
        if len(out) >= top_n:
            break
    return out
```

- [ ] **Step 3.4: Run tests**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestFilterAndRank -v`
Expected: PASS (7 tests).

- [ ] **Step 3.5: Commit**

```bash
git add src/polycopy/domain/discovery.py tests/unit/domain/test_discovery.py
git commit -m "feat(domain): add filter_and_rank with dedup and seed exclusion"
```

---

## Task 4: `render_candidates_yaml` pure function

**Files:**
- Modify: `src/polycopy/domain/discovery.py` (append function)
- Modify: `tests/unit/domain/test_discovery.py` (append test class)

- [ ] **Step 4.1: Append failing tests**

Append to `tests/unit/domain/test_discovery.py`:

```python
from pathlib import Path

from polycopy.domain.discovery import render_candidates_yaml
from polycopy.infrastructure.wallets_seed import load_wallets_seed


class TestRenderCandidatesYaml:
    def _candidate(self, addr_hex: str, label: str) -> CandidateWallet:
        return CandidateWallet(
            address=WalletAddress(value="0x" + addr_hex),
            label=label,
            rank=1,
            volume_usdc=Decimal("1000"),
            pnl_usdc=Decimal("100"),
            verified_badge=True,
        )

    def test_empty_list(self) -> None:
        assert render_candidates_yaml([]) == "wallets: []\n"

    def test_single_candidate_shape(self) -> None:
        out = render_candidates_yaml([self._candidate("a" * 40, "alice")])
        assert "wallets:" in out
        assert "address: \"0x" + "a" * 40 + "\"" in out
        assert "label: \"alice\"" in out

    def test_roundtrip_via_load_wallets_seed(self, tmp_path: Path) -> None:
        candidates = [
            self._candidate("a" * 40, "alice"),
            self._candidate("b" * 40, "bob"),
        ]
        yaml_text = render_candidates_yaml(candidates)
        path = tmp_path / "out.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        loaded = load_wallets_seed(path)
        assert len(loaded) == 2
        assert loaded[0].address.value == "0x" + "a" * 40
        assert loaded[0].label == "alice"
        assert loaded[1].label == "bob"
```

- [ ] **Step 4.2: Run tests to see them fail**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestRenderCandidatesYaml -v`
Expected: ImportError.

- [ ] **Step 4.3: Append `render_candidates_yaml`**

Append to `src/polycopy/domain/discovery.py`:

```python
def render_candidates_yaml(candidates: list[CandidateWallet]) -> str:
    """Render candidates as YAML matching wallets_seed.yaml schema."""
    if not candidates:
        return "wallets: []\n"
    lines = ["wallets:"]
    for c in candidates:
        lines.append(f"  - address: \"{c.address.value}\"")
        lines.append(f"    label: \"{c.label}\"")
    return "\n".join(lines) + "\n"
```

- [ ] **Step 4.4: Run tests**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestRenderCandidatesYaml -v`
Expected: PASS (3 tests).

- [ ] **Step 4.5: Commit**

```bash
git add src/polycopy/domain/discovery.py tests/unit/domain/test_discovery.py
git commit -m "feat(domain): render candidates YAML matching wallets_seed schema"
```

---

## Task 5: `render_report_md` pure function

**Files:**
- Modify: `src/polycopy/domain/discovery.py` (append function + dataclass `ReportMetadata`)
- Modify: `tests/unit/domain/test_discovery.py` (append test class)

- [ ] **Step 5.1: Append failing tests**

Append to `tests/unit/domain/test_discovery.py`:

```python
from datetime import UTC, datetime

from polycopy.domain.discovery import ReportMetadata, render_report_md


class TestRenderReportMd:
    def _candidate(self, addr_hex: str, label: str, vol: str, pnl: str, verified: bool = True) -> CandidateWallet:
        return CandidateWallet(
            address=WalletAddress(value="0x" + addr_hex),
            label=label,
            rank=1,
            volume_usdc=Decimal(vol),
            pnl_usdc=Decimal(pnl),
            verified_badge=verified,
        )

    def _meta(self) -> ReportMetadata:
        return ReportMetadata(
            generated_at=datetime(2026, 5, 4, 20, 30, 0, tzinfo=UTC),
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            order_by=OrderBy.PNL,
            min_volume_usdc=Decimal("5000"),
            top_requested=50,
            seed_path="config/wallets_seed.yaml",
            seed_size=2,
            total_fetched=50,
            total_excluded_existing=1,
            total_excluded_min_volume=4,
            total_candidates=45,
        )

    def test_frontmatter_contains_all_fields(self) -> None:
        candidates = [self._candidate("a" * 40, "alice", "1000", "100")]
        out = render_report_md(candidates, metadata=self._meta())
        # YAML frontmatter
        assert out.startswith("---\n")
        assert "generated_at: 2026-05-04T20:30:00+00:00" in out
        assert "time_period: MONTH" in out
        assert "category: OVERALL" in out
        assert "order_by: PNL" in out
        assert "min_volume_usdc: 5000" in out
        assert "top: 50" in out
        assert "seed_path: config/wallets_seed.yaml" in out
        assert "seed_size: 2" in out
        assert "total_fetched: 50" in out
        assert "total_excluded_existing: 1" in out
        assert "total_excluded_min_volume: 4" in out
        assert "total_candidates: 45" in out

    def test_table_has_one_row_per_candidate_plus_header(self) -> None:
        candidates = [
            self._candidate("a" * 40, "alice", "1000", "100"),
            self._candidate("b" * 40, "bob", "2000", "200"),
        ]
        out = render_report_md(candidates, metadata=self._meta())
        # Markdown table rows start with "|"; count after the header separator
        body = out.split("|---")[1] if "|---" in out else out
        rows = [line for line in body.splitlines() if line.startswith("|")]
        assert len(rows) == 2

    def test_pipes_in_label_escaped(self) -> None:
        candidates = [self._candidate("a" * 40, "a|b|c", "1000", "100")]
        out = render_report_md(candidates, metadata=self._meta())
        # Pipes in cell content must be escaped to not break markdown table
        assert "a\\|b\\|c" in out

    def test_verified_renders_yes_or_no(self) -> None:
        cands = [
            self._candidate("a" * 40, "alice", "1000", "100", verified=True),
            self._candidate("b" * 40, "bob", "1000", "100", verified=False),
        ]
        out = render_report_md(cands, metadata=self._meta())
        assert "yes" in out
        assert "no" in out

    def test_empty_candidates_list(self) -> None:
        out = render_report_md([], metadata=self._meta())
        # Frontmatter still rendered; table has only header
        assert out.startswith("---\n")
        assert "total_candidates: 45" in out  # metadata is the source of truth, not len()
```

- [ ] **Step 5.2: Run tests to see them fail**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestRenderReportMd -v`
Expected: ImportError on `ReportMetadata` and `render_report_md`.

- [ ] **Step 5.3: Append dataclass + function**

Append to `src/polycopy/domain/discovery.py`:

```python
from datetime import datetime  # add to imports near top


@dataclass(frozen=True)
class ReportMetadata:
    generated_at: datetime
    time_period: TimePeriod
    category: Category
    order_by: OrderBy
    min_volume_usdc: Decimal
    top_requested: int
    seed_path: str
    seed_size: int
    total_fetched: int
    total_excluded_existing: int
    total_excluded_min_volume: int
    total_candidates: int


def _escape_md_cell(text: str) -> str:
    """Escape pipe chars so they don't break a markdown table row."""
    return text.replace("|", "\\|")


def render_report_md(
    candidates: list[CandidateWallet],
    *,
    metadata: ReportMetadata,
) -> str:
    """Render the human-readable run report (frontmatter + markdown table)."""
    m = metadata
    fm = [
        "---",
        f"generated_at: {m.generated_at.isoformat()}",
        f"time_period: {m.time_period.value}",
        f"category: {m.category.value}",
        f"order_by: {m.order_by.value}",
        f"min_volume_usdc: {m.min_volume_usdc}",
        f"top: {m.top_requested}",
        f"seed_path: {m.seed_path}",
        f"seed_size: {m.seed_size}",
        f"total_fetched: {m.total_fetched}",
        f"total_excluded_existing: {m.total_excluded_existing}",
        f"total_excluded_min_volume: {m.total_excluded_min_volume}",
        f"total_candidates: {m.total_candidates}",
        "---",
        "",
        f"# Wallet candidates — {m.time_period.value}/{m.category.value} "
        f"(run {m.generated_at:%Y-%m-%d %H:%M UTC})",
        "",
        "| Rank | userName | Address | Volume (USDC) | PnL (USDC) | Verified | Polymarket |",
        "|-----:|----------|---------|--------------:|-----------:|:--------:|------------|",
    ]
    rows: list[str] = []
    for c in candidates:
        addr = c.address.value
        addr_short = f"{addr[:10]}…{addr[-4:]}"
        verified = "yes" if c.verified_badge else "no"
        link = f"https://polymarket.com/profile/{addr}"
        rows.append(
            f"| {c.rank} | {_escape_md_cell(c.label)} | {addr_short} | "
            f"{c.volume_usdc:,.2f} | {c.pnl_usdc:+,.2f} | {verified} | "
            f"[link]({link}) |"
        )
    return "\n".join(fm + rows) + "\n"
```

- [ ] **Step 5.4: Run tests**

Run: `uv run pytest tests/unit/domain/test_discovery.py::TestRenderReportMd -v`
Expected: PASS (5 tests).

- [ ] **Step 5.5: Commit**

```bash
git add src/polycopy/domain/discovery.py tests/unit/domain/test_discovery.py
git commit -m "feat(domain): render run report markdown with frontmatter and table"
```

---

## Task 6: Add metrics for leaderboard client

**Files:**
- Modify: `src/polycopy/infrastructure/observability/metrics.py:14-65,68-272`
- Test: extend an existing metrics test if present, otherwise add a tiny smoke test inline.

- [ ] **Step 6.1: Add field declarations**

Edit `src/polycopy/infrastructure/observability/metrics.py`. After line 65 (just before the `def make_metrics`), add inside the `Metrics` dataclass:

```python
    # Leaderboard discovery (Fase 6)
    leaderboard_requests_total: Counter
    leaderboard_request_duration_seconds: Histogram
```

- [ ] **Step 6.2: Add factory entries**

Inside `make_metrics()`, just before the closing `)` of `return Metrics(`, add:

```python
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
```

- [ ] **Step 6.3: Add smoke test**

Create `tests/unit/infrastructure/test_metrics_leaderboard.py`:

```python
from __future__ import annotations

from prometheus_client import CollectorRegistry

from polycopy.infrastructure.observability.metrics import make_metrics


def test_make_metrics_includes_leaderboard_metrics() -> None:
    metrics = make_metrics(registry=CollectorRegistry())
    metrics.leaderboard_requests_total.labels(endpoint="leaderboard", status="200").inc()
    metrics.leaderboard_request_duration_seconds.labels(endpoint="leaderboard").observe(0.1)
```

- [ ] **Step 6.4: Run test + full unit suite**

Run: `uv run pytest tests/unit/infrastructure/test_metrics_leaderboard.py -v`
Expected: PASS.

Then run: `uv run pytest tests/unit -v`
Expected: PASS (all existing unit tests still green; mypy strict checks pass).

- [ ] **Step 6.5: Commit**

```bash
git add src/polycopy/infrastructure/observability/metrics.py tests/unit/infrastructure/test_metrics_leaderboard.py
git commit -m "feat(observability): add leaderboard request counters and duration histogram"
```

---

## Task 7: `PolymarketLeaderboardPort` Protocol

**Files:**
- Create: `src/polycopy/ports/polymarket_leaderboard.py`
- (No test file — Protocol has no behavior to test directly. Adapter test in Task 8 covers it.)

- [ ] **Step 7.1: Create the Port**

Create `src/polycopy/ports/polymarket_leaderboard.py`:

```python
"""Port for Polymarket leaderboard endpoint (/v1/leaderboard)."""

from __future__ import annotations

from typing import Protocol

from polycopy.domain.discovery import (
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)


class PolymarketLeaderboardPort(Protocol):
    """Read-only access to Polymarket's public trader leaderboard."""

    async def fetch_leaderboard(
        self,
        *,
        time_period: TimePeriod,
        category: Category,
        order_by: OrderBy = OrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
    ) -> list[LeaderboardEntry]: ...
```

- [ ] **Step 7.2: Verify mypy is happy**

Run: `uv run mypy src/polycopy/ports/polymarket_leaderboard.py`
Expected: `Success: no issues found`.

- [ ] **Step 7.3: Commit**

```bash
git add src/polycopy/ports/polymarket_leaderboard.py
git commit -m "feat(ports): add PolymarketLeaderboardPort Protocol"
```

---

## Task 8: `PolymarketLeaderboardClient` adapter (httpx + tenacity)

**Files:**
- Create: `src/polycopy/infrastructure/polymarket/leaderboard_client.py`
- Test: `tests/unit/infrastructure/test_leaderboard_client.py`

- [ ] **Step 8.1: Write failing tests**

Create `tests/unit/infrastructure/test_leaderboard_client.py`:

```python
from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from prometheus_client import CollectorRegistry

from polycopy.domain.discovery import Category, OrderBy, TimePeriod
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.leaderboard_client import (
    PolymarketLeaderboardClient,
)


def _client(transport: httpx.MockTransport) -> PolymarketLeaderboardClient:
    metrics = make_metrics(registry=CollectorRegistry())
    return PolymarketLeaderboardClient(
        base_url="https://data-api.polymarket.com",
        metrics=metrics,
        transport=transport,
        timeout_s=1.0,
        max_retries=3,
    )


@pytest.mark.asyncio
async def test_fetch_parses_payload() -> None:
    payload = [
        {
            "rank": "1",
            "proxyWallet": "0x" + "a" * 40,
            "userName": "alice",
            "vol": 12345.67,
            "pnl": 999.99,
            "verifiedBadge": True,
        },
        {
            "rank": "2",
            "proxyWallet": "0x" + "b" * 40,
            "userName": None,
            "vol": 0,
            "pnl": -1.23,
            # verifiedBadge missing on purpose -> should default to False
        },
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/leaderboard"
        params = dict(request.url.params)
        assert params["timePeriod"] == "MONTH"
        assert params["category"] == "OVERALL"
        assert params["orderBy"] == "PNL"
        assert params["limit"] == "50"
        assert params["offset"] == "0"
        return httpx.Response(200, json=payload)

    client = _client(httpx.MockTransport(handler))
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.MONTH,
        category=Category.OVERALL,
        order_by=OrderBy.PNL,
        limit=50,
        offset=0,
    )
    assert len(rows) == 2
    assert rows[0].rank == 1
    assert rows[0].user_name == "alice"
    assert rows[0].volume_usdc == Decimal("12345.67")
    assert rows[0].pnl_usdc == Decimal("999.99")
    assert rows[0].verified_badge is True
    assert rows[1].user_name is None
    assert rows[1].verified_badge is False
    assert rows[1].pnl_usdc == Decimal("-1.23")


@pytest.mark.asyncio
async def test_retry_on_5xx_then_success() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json=[])

    client = _client(httpx.MockTransport(handler))
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.WEEK, category=Category.OVERALL,
    )
    assert rows == []
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_persistent_5xx_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_leaderboard(
            time_period=TimePeriod.WEEK, category=Category.OVERALL,
        )


@pytest.mark.asyncio
async def test_4xx_no_retry() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(400, text="bad")

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_leaderboard(
            time_period=TimePeriod.WEEK, category=Category.OVERALL,
        )
    assert calls["n"] == 1


@pytest.mark.asyncio
async def test_rank_string_or_int_both_parsed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[
            {"rank": 1, "proxyWallet": "0x" + "a" * 40, "userName": "x",
             "vol": 0, "pnl": 0, "verifiedBadge": False},
            {"rank": "2", "proxyWallet": "0x" + "b" * 40, "userName": "y",
             "vol": 0, "pnl": 0, "verifiedBadge": False},
        ])

    client = _client(httpx.MockTransport(handler))
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.WEEK, category=Category.OVERALL,
    )
    assert rows[0].rank == 1
    assert rows[1].rank == 2
```

- [ ] **Step 8.2: Run tests to see them fail**

Run: `uv run pytest tests/unit/infrastructure/test_leaderboard_client.py -v`
Expected: ModuleNotFoundError.

- [ ] **Step 8.3: Implement adapter**

Create `src/polycopy/infrastructure/polymarket/leaderboard_client.py`:

```python
"""PolymarketLeaderboardClient: httpx + tenacity + Prometheus metrics.

Endpoint: https://data-api.polymarket.com/v1/leaderboard
Retry: exponential backoff on 5xx and httpx.RequestError. No retry on 4xx.
Same shape as PolymarketDataClient (data_client.py).
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polycopy.domain.discovery import (
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.observability.metrics import Metrics


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return 500 <= exc.response.status_code < 600
    return isinstance(exc, httpx.RequestError)


class PolymarketLeaderboardClient:
    """Implements PolymarketLeaderboardPort."""

    def __init__(
        self,
        *,
        base_url: str,
        metrics: Metrics,
        transport: httpx.AsyncBaseTransport | httpx.BaseTransport | None = None,
        timeout_s: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._metrics = metrics
        self._transport = transport
        self._timeout_s = timeout_s
        self._max_retries = max_retries

    async def fetch_leaderboard(
        self,
        *,
        time_period: TimePeriod,
        category: Category,
        order_by: OrderBy = OrderBy.PNL,
        limit: int = 50,
        offset: int = 0,
    ) -> list[LeaderboardEntry]:
        params: dict[str, Any] = {
            "timePeriod": time_period.value,
            "category": category.value,
            "orderBy": order_by.value,
            "limit": limit,
            "offset": offset,
        }

        async def _do() -> httpx.Response:
            async with httpx.AsyncClient(
                timeout=self._timeout_s, transport=self._transport,
            ) as client:
                response = await client.get(
                    f"{self._base_url}/v1/leaderboard", params=params,
                )
                response.raise_for_status()
                return response

        start = time.perf_counter()
        try:
            response = await self._with_retry(_do)
        except httpx.HTTPStatusError as exc:
            self._metrics.leaderboard_requests_total.labels(
                endpoint="leaderboard", status=str(exc.response.status_code),
            ).inc()
            raise
        finally:
            self._metrics.leaderboard_request_duration_seconds.labels(
                endpoint="leaderboard",
            ).observe(time.perf_counter() - start)

        self._metrics.leaderboard_requests_total.labels(
            endpoint="leaderboard", status=str(response.status_code),
        ).inc()

        rows = response.json()
        return [self._row_to_entry(row) for row in rows]

    async def _with_retry(
        self, fn: Callable[[], Awaitable[httpx.Response]],
    ) -> httpx.Response:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._max_retries),
            wait=wait_exponential(multiplier=0.1, max=2),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        ):
            with attempt:
                return await fn()
        raise RuntimeError("unreachable")

    @staticmethod
    def _row_to_entry(row: dict[str, Any]) -> LeaderboardEntry:
        rank_raw = row["rank"]
        rank = int(rank_raw) if not isinstance(rank_raw, int) else rank_raw
        return LeaderboardEntry(
            rank=rank,
            address=WalletAddress(value=row["proxyWallet"]),
            user_name=row.get("userName"),
            volume_usdc=Decimal(str(row.get("vol", 0))),
            pnl_usdc=Decimal(str(row.get("pnl", 0))),
            verified_badge=bool(row.get("verifiedBadge", False)),
        )
```

- [ ] **Step 8.4: Run tests**

Run: `uv run pytest tests/unit/infrastructure/test_leaderboard_client.py -v`
Expected: PASS (5 tests).

- [ ] **Step 8.5: Commit**

```bash
git add src/polycopy/infrastructure/polymarket/leaderboard_client.py tests/unit/infrastructure/test_leaderboard_client.py
git commit -m "feat(infra): add PolymarketLeaderboardClient with retry and metrics"
```

---

## Task 9: CLI script `discover_wallets.py`

**Files:**
- Create: `src/polycopy/scripts/discover_wallets.py`
- Test: `tests/unit/scripts/test_discover_wallets_cli.py`

- [ ] **Step 9.1: Write failing tests**

Create `tests/unit/scripts/test_discover_wallets_cli.py`:

```python
from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from polycopy.domain.discovery import (
    Category,
    LeaderboardEntry,
    OrderBy,
    TimePeriod,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.scripts.discover_wallets import (
    DiscoverArgs,
    parse_args,
    run_discover,
)


SEED_YAML = """\
wallets:
  - address: "0xa5ea13a81d2b7e8e424b182bdc1db08e756bd96a"
    label: "bossoskil1"
"""


class FakeLeaderboard:
    def __init__(self, pages: list[list[LeaderboardEntry]]) -> None:
        self._pages = pages
        self.calls: list[dict[str, Any]] = []

    async def fetch_leaderboard(
        self, *, time_period: TimePeriod, category: Category,
        order_by: OrderBy = OrderBy.PNL, limit: int = 50, offset: int = 0,
    ) -> list[LeaderboardEntry]:
        self.calls.append({"limit": limit, "offset": offset})
        idx = offset // limit
        return self._pages[idx] if idx < len(self._pages) else []


def _entry(addr_hex: str, vol: str, pnl: str, name: str = "user") -> LeaderboardEntry:
    return LeaderboardEntry(
        rank=1,
        address=WalletAddress(value="0x" + addr_hex),
        user_name=name,
        volume_usdc=Decimal(vol),
        pnl_usdc=Decimal(pnl),
        verified_badge=False,
    )


class TestParseArgs:
    def test_defaults(self) -> None:
        args = parse_args([])
        assert args.time_period == TimePeriod.MONTH
        assert args.category == Category.OVERALL
        assert args.top == 50
        assert args.min_volume_usdc == Decimal("5000")
        assert args.dry_run is False

    def test_top_clamped_with_warning(self, capsys: pytest.CaptureFixture[str]) -> None:
        args = parse_args(["--top", "9999"])
        captured = capsys.readouterr()
        assert args.top == 1050
        assert "clamped" in captured.err.lower()

    def test_invalid_time_period(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["--time-period", "FOO"])


@pytest.mark.asyncio
class TestRunDiscover:
    async def test_writes_outputs(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(pages=[[
            _entry("b" * 40, "10000", "500", name="alice"),
            _entry("c" * 40, "10000", "400", name="bob"),
        ]])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH,
            category=Category.OVERALL,
            top=50,
            min_volume_usdc=Decimal("5000"),
            seed_path=seed_path,
            candidates_out=cands_out,
            report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        assert cands_out.exists()
        assert report_out.exists()
        text = cands_out.read_text(encoding="utf-8")
        assert "0x" + "b" * 40 in text
        assert "0x" + "c" * 40 in text

    async def test_excludes_seed_wallet(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        seeded_addr = "a5ea13a81d2b7e8e424b182bdc1db08e756bd96a"
        leaderboard = FakeLeaderboard(pages=[[
            _entry(seeded_addr, "10000", "500"),
            _entry("b" * 40, "10000", "400"),
        ]])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH, category=Category.OVERALL,
            top=50, min_volume_usdc=Decimal("0"),
            seed_path=seed_path, candidates_out=cands_out, report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        text = cands_out.read_text(encoding="utf-8")
        assert "0x" + seeded_addr not in text
        assert "0x" + "b" * 40 in text

    async def test_dry_run_does_not_write(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(pages=[[
            _entry("b" * 40, "10000", "500"),
        ]])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH, category=Category.OVERALL,
            top=50, min_volume_usdc=Decimal("0"),
            seed_path=seed_path, candidates_out=cands_out, report_out=report_out,
            dry_run=True,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        assert not cands_out.exists()
        assert not report_out.exists()

    async def test_no_candidates_after_filters_exit_2(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text(SEED_YAML, encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        leaderboard = FakeLeaderboard(pages=[[
            _entry("b" * 40, "100", "500"),  # below min_volume
        ]])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH, category=Category.OVERALL,
            top=50, min_volume_usdc=Decimal("5000"),
            seed_path=seed_path, candidates_out=cands_out, report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 2
        assert not cands_out.exists()
        assert not report_out.exists()

    async def test_paginates_until_top(self, tmp_path: Path) -> None:
        seed_path = tmp_path / "seed.yaml"
        seed_path.write_text("wallets: []\n", encoding="utf-8")
        cands_out = tmp_path / "candidates.yaml"
        report_out = tmp_path / "report.md"

        page0 = [_entry(f"{i:040x}", "10000", "100") for i in range(50)]
        page1 = [_entry(f"{i:040x}", "10000", "100") for i in range(50, 75)]
        leaderboard = FakeLeaderboard(pages=[page0, page1])

        args = DiscoverArgs(
            time_period=TimePeriod.MONTH, category=Category.OVERALL,
            top=70, min_volume_usdc=Decimal("0"),
            seed_path=seed_path, candidates_out=cands_out, report_out=report_out,
            dry_run=False,
        )
        exit_code = await run_discover(args, leaderboard)
        assert exit_code == 0
        # Two pages requested
        assert len(leaderboard.calls) == 2
        assert leaderboard.calls[0]["offset"] == 0
        assert leaderboard.calls[1]["offset"] == 50
```

- [ ] **Step 9.2: Run tests to see them fail**

Run: `uv run pytest tests/unit/scripts/test_discover_wallets_cli.py -v`
Expected: ModuleNotFoundError.

- [ ] **Step 9.3: Implement the script**

Create `src/polycopy/scripts/discover_wallets.py`:

```python
"""Discover wallets CLI — queries Polymarket leaderboard and emits candidates.

Usage:
    uv run python -m polycopy.scripts.discover_wallets [flags]

Flags (all override-able):
    --time-period {DAY,WEEK,MONTH,ALL}     default: MONTH
    --category {OVERALL,POLITICS,...}      default: OVERALL
    --top N                                default: 50  (clamped to 1050)
    --min-volume USDC                      default: 5000
    --seed-path PATH                       default: config/wallets_seed.yaml
    --candidates-out PATH                  default: config/wallets_candidates.yaml
    --report-out PATH                      default: docs/discover_wallets_report.md
    --dry-run                              prints table only, no files

Exit codes:
    0  success
    1  fatal error (API/IO failure)
    2  no candidates after filtering (no files written)
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from polycopy.domain.discovery import (
    Category,
    OrderBy,
    ReportMetadata,
    TimePeriod,
    filter_and_rank,
    render_candidates_yaml,
    render_report_md,
)
from polycopy.domain.value_objects import WalletAddress
from polycopy.infrastructure.wallets_seed import load_wallets_seed
from polycopy.ports.polymarket_leaderboard import PolymarketLeaderboardPort

DEFAULT_SEED_PATH = Path("config/wallets_seed.yaml")
DEFAULT_CANDIDATES_OUT = Path("config/wallets_candidates.yaml")
DEFAULT_REPORT_OUT = Path("docs/discover_wallets_report.md")
PAGE_SIZE = 50
MAX_TOP = 1050  # API allows offset 0..1000, page size up to 50.


@dataclass(frozen=True)
class DiscoverArgs:
    time_period: TimePeriod
    category: Category
    top: int
    min_volume_usdc: Decimal
    seed_path: Path
    candidates_out: Path
    report_out: Path
    dry_run: bool


def parse_args(argv: list[str] | None = None) -> DiscoverArgs:
    parser = argparse.ArgumentParser(prog="discover_wallets")
    parser.add_argument("--time-period", default="MONTH",
                        choices=[tp.value for tp in TimePeriod])
    parser.add_argument("--category", default="OVERALL",
                        choices=[c.value for c in Category])
    parser.add_argument("--top", type=int, default=50)
    parser.add_argument("--min-volume", type=Decimal, default=Decimal("5000"),
                        dest="min_volume_usdc")
    parser.add_argument("--seed-path", type=Path, default=DEFAULT_SEED_PATH)
    parser.add_argument("--candidates-out", type=Path, default=DEFAULT_CANDIDATES_OUT)
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT_OUT)
    parser.add_argument("--dry-run", action="store_true")
    ns = parser.parse_args(argv)

    top = ns.top
    if top > MAX_TOP:
        print(
            f"warning: --top {top} clamped to {MAX_TOP} (API offset cap)",
            file=sys.stderr,
        )
        top = MAX_TOP
    if top < 1:
        parser.error("--top must be >= 1")

    return DiscoverArgs(
        time_period=TimePeriod(ns.time_period),
        category=Category(ns.category),
        top=top,
        min_volume_usdc=ns.min_volume_usdc,
        seed_path=ns.seed_path,
        candidates_out=ns.candidates_out,
        report_out=ns.report_out,
        dry_run=ns.dry_run,
    )


async def run_discover(
    args: DiscoverArgs, leaderboard: PolymarketLeaderboardPort,
) -> int:
    seed = load_wallets_seed(args.seed_path)
    seed_addrs: set[WalletAddress] = {w.address for w in seed}

    fetched = []
    offset = 0
    while len(fetched) < args.top and offset <= 1000:
        page = await leaderboard.fetch_leaderboard(
            time_period=args.time_period,
            category=args.category,
            order_by=OrderBy.PNL,
            limit=PAGE_SIZE,
            offset=offset,
        )
        fetched.extend(page)
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE

    excluded_existing = sum(1 for e in fetched if e.address in seed_addrs)
    excluded_min_vol = sum(
        1 for e in fetched
        if e.address not in seed_addrs and e.volume_usdc < args.min_volume_usdc
    )

    candidates = filter_and_rank(
        fetched,
        min_volume_usdc=args.min_volume_usdc,
        exclude=seed_addrs,
        top_n=args.top,
    )

    if not candidates:
        if not fetched:
            print(
                f"error: no rows from API for time_period={args.time_period.value} "
                f"category={args.category.value}",
                file=sys.stderr,
            )
        else:
            print(
                f"error: all {len(fetched)} fetched rows were excluded "
                f"(by seed: {excluded_existing}, by min_volume: {excluded_min_vol})",
                file=sys.stderr,
            )
        return 2

    _print_table(candidates)

    if args.dry_run:
        return 0

    metadata = ReportMetadata(
        generated_at=datetime.now(tz=UTC),
        time_period=args.time_period,
        category=args.category,
        order_by=OrderBy.PNL,
        min_volume_usdc=args.min_volume_usdc,
        top_requested=args.top,
        seed_path=str(args.seed_path),
        seed_size=len(seed),
        total_fetched=len(fetched),
        total_excluded_existing=excluded_existing,
        total_excluded_min_volume=excluded_min_vol,
        total_candidates=len(candidates),
    )

    args.candidates_out.write_text(
        render_candidates_yaml(candidates), encoding="utf-8",
    )
    args.report_out.write_text(
        render_report_md(candidates, metadata=metadata), encoding="utf-8",
    )
    return 0


def _print_table(candidates: list) -> None:  # type: ignore[type-arg]
    print(f"{'rank':>4}  {'label':<24}  {'address':<44}  {'volume':>14}  {'pnl':>12}")
    print("-" * 110)
    for c in candidates:
        print(
            f"{c.rank:>4}  {c.label[:24]:<24}  {c.address.value:<44}  "
            f"{c.volume_usdc:>14,.2f}  {c.pnl_usdc:>+12,.2f}"
        )


async def _async_main(argv: list[str] | None = None) -> int:
    from polycopy.config import Settings
    from polycopy.infrastructure.observability.logging import configure_logging
    from polycopy.infrastructure.observability.metrics import make_metrics
    from polycopy.infrastructure.polymarket.leaderboard_client import (
        PolymarketLeaderboardClient,
    )

    args = parse_args(argv)
    settings = Settings()
    configure_logging(env=settings.env, level=settings.log_level)
    metrics = make_metrics()
    client = PolymarketLeaderboardClient(
        base_url=settings.polymarket_base_url, metrics=metrics,
    )
    return await run_discover(args, client)


def main() -> None:
    sys.exit(asyncio.run(_async_main()))


if __name__ == "__main__":
    main()
```

- [ ] **Step 9.4: Run tests**

Run: `uv run pytest tests/unit/scripts/test_discover_wallets_cli.py -v`
Expected: PASS (5 + 3 = 8 tests).

- [ ] **Step 9.5: Run full unit suite + mypy**

Run: `uv run pytest tests/unit -v && uv run mypy src`
Expected: PASS / `Success: no issues found`.

- [ ] **Step 9.6: Commit**

```bash
git add src/polycopy/scripts/discover_wallets.py tests/unit/scripts/test_discover_wallets_cli.py
git commit -m "feat(scripts): add discover_wallets CLI with pagination and dry-run"
```

---

## Task 10: Live integration test (gated)

**Files:**
- Create: `tests/integration/test_leaderboard_live.py`

- [ ] **Step 10.1: Write the gated test**

Create `tests/integration/test_leaderboard_live.py`:

```python
"""Live integration test against Polymarket /v1/leaderboard.

Gated by env var to avoid CI dependency on third-party uptime.
Run locally with:

    PYTEST_LIVE_POLYMARKET=1 uv run pytest tests/integration/test_leaderboard_live.py -v
"""

from __future__ import annotations

import os

import pytest
from prometheus_client import CollectorRegistry

from polycopy.domain.discovery import Category, OrderBy, TimePeriod
from polycopy.infrastructure.observability.metrics import make_metrics
from polycopy.infrastructure.polymarket.leaderboard_client import (
    PolymarketLeaderboardClient,
)


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("PYTEST_LIVE_POLYMARKET") != "1",
    reason="set PYTEST_LIVE_POLYMARKET=1 to run live integration",
)
async def test_leaderboard_live_smoke() -> None:
    metrics = make_metrics(registry=CollectorRegistry())
    client = PolymarketLeaderboardClient(
        base_url="https://data-api.polymarket.com", metrics=metrics, timeout_s=15.0,
    )
    rows = await client.fetch_leaderboard(
        time_period=TimePeriod.MONTH,
        category=Category.OVERALL,
        order_by=OrderBy.PNL,
        limit=5,
        offset=0,
    )
    assert len(rows) <= 5
    if rows:
        first = rows[0]
        assert first.address.value.startswith("0x")
        assert len(first.address.value) == 42
        assert first.rank >= 1
```

- [ ] **Step 10.2: Confirm it skips by default**

Run: `uv run pytest tests/integration/test_leaderboard_live.py -v`
Expected: 1 skipped.

- [ ] **Step 10.3: Run live (manual sanity check)**

Run: `PYTEST_LIVE_POLYMARKET=1 uv run pytest tests/integration/test_leaderboard_live.py -v`
Expected: PASS (real network call to polymarket; 1 row or more parsed).

If the network is unreachable from your dev machine (e.g., Hetzner geofence), this is expected — note it in the commit message and proceed.

- [ ] **Step 10.4: Commit**

```bash
git add tests/integration/test_leaderboard_live.py
git commit -m "test(integration): add gated live smoke for leaderboard endpoint"
```

---

## Task 11: Final verification + manual smoke

**No file changes — verification only.**

- [ ] **Step 11.1: Full test suite**

Run: `uv run pytest tests -v --ignore=tests/integration/test_leaderboard_live.py`
Expected: All previously-passing tests still pass; new ones from Tasks 1–9 pass.

- [ ] **Step 11.2: Mypy strict on full src tree**

Run: `uv run mypy src`
Expected: `Success: no issues found`.

- [ ] **Step 11.3: Manual CLI dry-run against the real API**

Run: `uv run python -m polycopy.scripts.discover_wallets --dry-run --top 5`
Expected: prints table of up to 5 candidates from `MONTH`/`OVERALL` (excluding `bossoskil1`/`Countryside` if returned). No files created. Exit 0.

If network blocked, document the limitation but consider task done — the CLI is exercised by unit tests with `FakeLeaderboard`.

- [ ] **Step 11.4: Manual CLI real run**

Run: `uv run python -m polycopy.scripts.discover_wallets --top 10`
Expected: writes `config/wallets_candidates.yaml` and `docs/discover_wallets_report.md`. Inspect both files.

Verify:
- `wallets_candidates.yaml` parseable by `load_wallets_seed`:
  `uv run python -c "from polycopy.infrastructure.wallets_seed import load_wallets_seed; from pathlib import Path; print(load_wallets_seed(Path('config/wallets_candidates.yaml')))"`
- `docs/discover_wallets_report.md` opens cleanly in a markdown viewer; frontmatter has all 13 fields.
- Re-run produces the same output (or near-same; leaderboard may shift slightly).

- [ ] **Step 11.5: Update auto-memory project file**

Update `/home/polycopy/.claude/projects/-home-polycopy-projects-polycopy/memory/project_active_plan.md` with: "Fase 6 (multi-wallet discovery CLI) concluída em 2026-MM-DD (head `<short-sha>`). CLI `python -m polycopy.scripts.discover_wallets` gera `config/wallets_candidates.yaml` + `docs/discover_wallets_report.md`."

- [ ] **Step 11.6: Final commit (if `wallets_candidates.yaml` and report were generated and look reasonable)**

This is **optional** and only if the user wants to track the first generated artifacts in git. Do **not** auto-commit `config/wallets_candidates.yaml` or `docs/discover_wallets_report.md` — both are output artifacts and the user may want to gitignore them. Ask the user.

---

## Self-Review

**Spec coverage check:**

| Spec section | Implemented in |
|---|---|
| §3.1 CLI one-shot | Task 9 |
| §3.2 PnL + filtros mínimos | Task 3 (`filter_and_rank`) |
| §3.3 defaults `MONTH`/`OVERALL`/top 50 | Task 9 (`parse_args`) |
| §3.4 Output dual + idempotente | Task 9 (`run_discover`) + Task 4 (YAML) + Task 5 (MD) |
| §3.5 Estrutura Port + adapter + domain puro + script thin | Tasks 7, 8, 1–5, 9 respectively |
| §3.6 Sem auto-promoção | Out of scope; nothing writes to seed |
| §3.7 Cap top=1050 | Task 9 (`MAX_TOP`) |
| §3.8 Sem DB/JetStream/container | Architecture; nothing in plan touches those |
| §4.1 Port | Task 7 |
| §4.2 Domain types & funções | Tasks 1–5 |
| §4.3 Adapter httpx + tenacity + métricas | Task 8 |
| §4.4 CLI argparse + flags | Task 9 |
| §4.5 Reuso | Task 9 imports `load_wallets_seed`, `Settings`, `make_metrics`, `configure_logging` |
| §5.1 Schema YAML | Task 4 (roundtrip test via `load_wallets_seed`) |
| §5.2 Schema MD | Task 5 (frontmatter test) |
| §6 Errors/edge cases | Task 8 (5xx, 4xx) + Task 9 (no candidates exit 2, dry-run, top clamp) |
| §7.1 Unit domain tests | Tasks 1–5 |
| §7.2 Unit adapter tests | Task 8 |
| §7.3 Unit CLI tests | Task 9 |
| §7.4 Live integration | Task 10 |
| §8.1 Métricas | Task 6 |
| §8.2 Logs estruturados | Task 9 (`configure_logging` chamado em `_async_main`); explicit `discover_run_*` log events deferred — `configure_logging` is enough for stdlib logging through the call path |
| §11 Critérios de aceite | Task 11 verifies all 7 |

**Note on §8.2 deferred:** the spec mentions specific structured log events (`discover_run_started`, `leaderboard_page_fetched`, etc.) that aren't explicit task steps. They are minor observability additions that won't change the contract. If the user wants them, they can be added as a follow-up commit after Task 9.

**Placeholder scan:** none.

**Type/name consistency:** `LeaderboardEntry`, `CandidateWallet`, `ReportMetadata`, `TimePeriod`, `Category`, `OrderBy`, `PolymarketLeaderboardPort`, `PolymarketLeaderboardClient`, `DiscoverArgs`, `parse_args`, `run_discover`, `derive_label`, `filter_and_rank`, `render_candidates_yaml`, `render_report_md`, `MAX_TOP`, `PAGE_SIZE` — all consistent across tasks.
