"""Testes unit dos value objects do Plano 5A."""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime

import pytest

from polycopy.domain.events import ResolvedOutcome
from polycopy.domain.resolution import MarketResolution, ResolvedMarketDTO

_VALID_COND = "0x" + "ab" * 32
_VALID_TOKEN_YES = "111"
_VALID_TOKEN_NO = "222"
_VALID_RAW = '["0.0", "1.0"]'


def _make_resolution(
    *,
    outcome: ResolvedOutcome = ResolvedOutcome.YES,
    winning_token_id: str | None = _VALID_TOKEN_YES,
    decided_at_naive: bool = False,
) -> MarketResolution:
    decided_at = datetime(2026, 5, 1) if decided_at_naive else datetime.now(tz=UTC)
    return MarketResolution(
        condition_id=_VALID_COND,
        resolved_outcome=outcome,
        winning_token_id=winning_token_id,
        closed_time=datetime.now(tz=UTC),
        resolved_at=decided_at,
        outcome_prices_raw=_VALID_RAW,
        uma_resolution_statuses_raw="[]",
    )


def test_resolved_outcome_values() -> None:
    assert ResolvedOutcome.YES.value == "YES"
    assert ResolvedOutcome.NO.value == "NO"
    assert ResolvedOutcome.INVALID.value == "INVALID"


def test_resolution_yes_with_winning_token_valid() -> None:
    r = _make_resolution(outcome=ResolvedOutcome.YES, winning_token_id=_VALID_TOKEN_YES)
    assert r.resolved_outcome == ResolvedOutcome.YES
    assert r.winning_token_id == _VALID_TOKEN_YES


def test_resolution_no_with_winning_token_valid() -> None:
    r = _make_resolution(outcome=ResolvedOutcome.NO, winning_token_id=_VALID_TOKEN_NO)
    assert r.winning_token_id == _VALID_TOKEN_NO


def test_resolution_invalid_without_winning_token_valid() -> None:
    r = _make_resolution(outcome=ResolvedOutcome.INVALID, winning_token_id=None)
    assert r.winning_token_id is None


def test_resolution_yes_without_winning_token_raises() -> None:
    with pytest.raises(ValueError, match="must have winning_token_id"):
        _make_resolution(outcome=ResolvedOutcome.YES, winning_token_id=None)


def test_resolution_no_without_winning_token_raises() -> None:
    with pytest.raises(ValueError, match="must have winning_token_id"):
        _make_resolution(outcome=ResolvedOutcome.NO, winning_token_id=None)


def test_resolution_invalid_with_winning_token_raises() -> None:
    with pytest.raises(ValueError, match="INVALID resolution must have winning_token_id=None"):
        _make_resolution(outcome=ResolvedOutcome.INVALID, winning_token_id=_VALID_TOKEN_YES)


def test_resolution_naive_resolved_at_raises() -> None:
    with pytest.raises(ValueError, match="resolved_at must be timezone-aware"):
        _make_resolution(decided_at_naive=True)


def test_resolution_naive_closed_time_raises() -> None:
    with pytest.raises(ValueError, match="closed_time must be timezone-aware"):
        MarketResolution(
            condition_id=_VALID_COND,
            resolved_outcome=ResolvedOutcome.YES,
            winning_token_id=_VALID_TOKEN_YES,
            closed_time=datetime(2026, 5, 1),  # naive
            resolved_at=datetime.now(tz=UTC),
            outcome_prices_raw=_VALID_RAW,
            uma_resolution_statuses_raw="[]",
        )


def test_resolution_closed_time_none_valid() -> None:
    r = MarketResolution(
        condition_id=_VALID_COND,
        resolved_outcome=ResolvedOutcome.YES,
        winning_token_id=_VALID_TOKEN_YES,
        closed_time=None,
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw=_VALID_RAW,
        uma_resolution_statuses_raw="[]",
    )
    assert r.closed_time is None


def test_resolution_uma_statuses_none_valid() -> None:
    r = MarketResolution(
        condition_id=_VALID_COND,
        resolved_outcome=ResolvedOutcome.YES,
        winning_token_id=_VALID_TOKEN_YES,
        closed_time=datetime.now(tz=UTC),
        resolved_at=datetime.now(tz=UTC),
        outcome_prices_raw=_VALID_RAW,
        uma_resolution_statuses_raw=None,
    )
    assert r.uma_resolution_statuses_raw is None


def test_resolution_frozen() -> None:
    r = _make_resolution()
    with pytest.raises(dataclasses.FrozenInstanceError):
        r.resolved_outcome = ResolvedOutcome.NO  # type: ignore[misc]


def test_resolved_market_dto_basic() -> None:
    dto = ResolvedMarketDTO(
        condition_id=_VALID_COND,
        yes_token_id=_VALID_TOKEN_YES,
        no_token_id=_VALID_TOKEN_NO,
        closed=True,
        closed_time=datetime.now(tz=UTC),
        outcome_prices_raw='["1.0", "0.0"]',
        uma_resolution_statuses_raw="[]",
    )
    assert dto.closed is True
    assert dto.condition_id == _VALID_COND


def test_resolved_market_dto_frozen() -> None:
    dto = ResolvedMarketDTO(
        condition_id=_VALID_COND,
        yes_token_id=_VALID_TOKEN_YES,
        no_token_id=_VALID_TOKEN_NO,
        closed=False,
        closed_time=None,
        outcome_prices_raw='["0.5", "0.5"]',
        uma_resolution_statuses_raw=None,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        dto.closed = True  # type: ignore[misc]
