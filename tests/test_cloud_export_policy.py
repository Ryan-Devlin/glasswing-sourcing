from __future__ import annotations

import filter as screening_filter


def test_bulk_exports_default_off_with_shared_postgres(monkeypatch):
    monkeypatch.delenv("GLASSWING_BULK_CSV_EXPORTS", raising=False)
    monkeypatch.setattr(
        screening_filter,
        "using_classification_postgres",
        lambda: True,
    )
    assert screening_filter.bulk_compatibility_exports_enabled() is False


def test_bulk_exports_remain_on_for_local_mode(monkeypatch):
    monkeypatch.delenv("GLASSWING_BULK_CSV_EXPORTS", raising=False)
    monkeypatch.setattr(
        screening_filter,
        "using_classification_postgres",
        lambda: False,
    )
    assert screening_filter.bulk_compatibility_exports_enabled() is True


def test_bulk_exports_can_be_explicitly_enabled(monkeypatch):
    monkeypatch.setenv("GLASSWING_BULK_CSV_EXPORTS", "1")
    monkeypatch.setattr(
        screening_filter,
        "using_classification_postgres",
        lambda: True,
    )
    assert screening_filter.bulk_compatibility_exports_enabled() is True
