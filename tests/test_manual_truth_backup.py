import json

import pandas as pd
import pytest

import common
import manual_truth_backup
import training_truth
from classification_store import (
    connect_store,
    record_manual_move,
    upsert_classifications,
)


def test_snapshot_contains_only_human_truth_and_detects_tampering(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "classifications.sqlite3"
    manual_root = tmp_path / "manual"
    manual_root.mkdir()
    pd.DataFrame(
        [
            {
                "domain": "human.ai",
                "label": "yes",
                "rating": "91",
                "source": "human",
                "reason": "reviewed",
                "labeled_at": "2026-01-01T00:00:00Z",
            },
            {
                "domain": "machine.ai",
                "label": "no",
                "rating": "0",
                "source": "python",
                "reason": "automatic",
                "labeled_at": "2026-01-01T00:00:00Z",
            },
        ]
    ).to_csv(manual_root / "training_label_metadata.csv", index=False)
    pd.DataFrame(
        [
            {
                "domain": "human.ai",
                "override_target": "Investable Candidate",
                "override_note": "reviewed",
                "updated_at": "2026-01-01T00:00:00Z",
            }
        ]
    ).to_csv(manual_root / "domain_overrides.csv", index=False)

    with connect_store(database):
        pass
    record_manual_move(
        ["human.ai"],
        target_label="Investable Candidate",
        target_bucket="priority",
        source="human",
        path=database,
    )
    record_manual_move(
        ["machine.ai"],
        target_label="Hard Denied",
        target_bucket="reject",
        source="python",
        path=database,
    )

    monkeypatch.setattr(manual_truth_backup, "MANUAL", manual_root)
    monkeypatch.setattr(
        manual_truth_backup,
        "read_label_metadata",
        lambda: pd.read_csv(
            manual_root / "training_label_metadata.csv",
            dtype=str,
        ).fillna(""),
    )
    snapshot = manual_truth_backup.create_manual_snapshot(
        reason="unit_test",
        classification_db=database,
        snapshot_root=tmp_path / "snapshots",
    )
    payload = manual_truth_backup.validate_manual_snapshot(snapshot)

    assert [row["domain"] for row in payload["manual_decisions"]] == [
        "human.ai"
    ]
    assert [row["domain"] for row in payload["movement_log"]] == ["human.ai"]
    assert [row["domain"] for row in payload["human_training_labels"]] == [
        "human.ai"
    ]
    assert payload["counts"]["domain_overrides"] == 1

    tampered = json.loads(snapshot.read_text(encoding="utf-8"))
    tampered["reason"] = "changed"
    snapshot.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        manual_truth_backup.validate_manual_snapshot(snapshot)


def test_restore_recovers_human_decision_without_erasing_history(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "classifications.sqlite3"
    manual_root = tmp_path / "manual"
    snapshots = manual_root / "snapshots"
    manual_root.mkdir()
    yes_path = manual_root / "yes_basket.csv"
    no_path = manual_root / "no_basket.csv"
    metadata_path = manual_root / "training_label_metadata.csv"
    overrides_path = manual_root / "domain_overrides.csv"
    ground_truth_path = manual_root / "human_ground_truth.csv"

    for module in (common,):
        monkeypatch.setattr(module, "MANUAL", manual_root)
        monkeypatch.setattr(module, "YES_BASKET_DATA", yes_path)
        monkeypatch.setattr(module, "NO_BASKET_DATA", no_path)
    monkeypatch.setattr(manual_truth_backup, "MANUAL", manual_root)
    monkeypatch.setattr(manual_truth_backup, "SNAPSHOT_ROOT", snapshots)
    monkeypatch.setattr(training_truth, "MANUAL", manual_root)
    monkeypatch.setattr(training_truth, "YES_BASKET_DATA", yes_path)
    monkeypatch.setattr(training_truth, "NO_BASKET_DATA", no_path)
    monkeypatch.setattr(training_truth, "OVERRIDES_CSV", overrides_path)
    monkeypatch.setattr(training_truth, "GROUND_TRUTH_CSV", ground_truth_path)

    pd.DataFrame(columns=["domain_url", "rating"]).to_csv(
        yes_path,
        index=False,
    )
    pd.DataFrame(columns=["domain_url", "rating"]).to_csv(
        no_path,
        index=False,
    )
    pd.DataFrame(
        columns=["domain", "override_target", "override_note", "updated_at"]
    ).to_csv(overrides_path, index=False)
    upsert_classifications(
        pd.DataFrame(
            [
                {
                    "domain": "restore.ai",
                    "routing_bucket": "review",
                    "homepage_excerpt": "Enterprise infrastructure",
                }
            ]
        ),
        database,
    )
    record_manual_move(
        ["restore.ai"],
        target_label="Investable Candidate",
        target_bucket="priority",
        scores={"restore.ai": 88},
        source="human",
        path=database,
    )
    pd.DataFrame(
        [
            {
                "domain": "restore.ai",
                "label": "yes",
                "rating": "88",
                "source": "human",
                "reason": "strong wedge",
                "labeled_at": "2026-01-01T00:00:00Z",
            }
        ]
    ).to_csv(metadata_path, index=False)
    target = manual_truth_backup.create_manual_snapshot(
        reason="restore_target",
        classification_db=database,
    )

    record_manual_move(
        ["restore.ai"],
        target_label="Hard Denied",
        target_bucket="reject",
        source="human",
        path=database,
    )
    pd.DataFrame(
        [
            {
                "domain": "restore.ai",
                "label": "no",
                "rating": "0",
                "source": "human",
                "reason": "changed",
                "labeled_at": "2026-02-01T00:00:00Z",
            }
        ]
    ).to_csv(metadata_path, index=False)

    result = manual_truth_backup.restore_manual_snapshot(
        target,
        classification_db=database,
    )

    assert result["restored_decisions"] == 1
    with connect_store(database, read_only=True) as conn:
        decision = conn.execute(
            "SELECT target_bucket FROM manual_decisions "
            "WHERE domain='restore.ai'"
        ).fetchone()
        classification = conn.execute(
            "SELECT routing_bucket FROM classifications "
            "WHERE domain='restore.ai'"
        ).fetchone()
        movements = conn.execute(
            "SELECT count(*) FROM movement_log WHERE domain='restore.ai'"
        ).fetchone()[0]
    restored_metadata = pd.read_csv(metadata_path, dtype=str)
    assert decision["target_bucket"] == "priority"
    assert classification["routing_bucket"] == "priority"
    assert movements == 3
    assert restored_metadata.iloc[0]["label"] == "yes"
    assert len(manual_truth_backup.list_manual_snapshots(snapshots)) == 3
