import pandas as pd
import pytest

from classification_store import record_manual_move, upsert_classifications
from model_evaluation import performance_metrics


def test_performance_metrics_measure_human_reversals(tmp_path):
    database = tmp_path / "classifications.sqlite3"
    upsert_classifications(
        pd.DataFrame(
            [
                {
                    "domain": "missed.ai",
                    "routing_bucket": "reject",
                    "homepage_excerpt": "Deep technical infrastructure",
                },
                {
                    "domain": "false-positive.com",
                    "routing_bucket": "priority",
                    "homepage_excerpt": "Generic product",
                },
                {
                    "domain": "correct.app",
                    "routing_bucket": "priority",
                    "homepage_excerpt": "Vertical AI platform",
                },
            ]
        ),
        database,
    )
    record_manual_move(
        ["missed.ai"],
        target_label="Investable Candidate",
        target_bucket="priority",
        source="human",
        path=database,
    )
    record_manual_move(
        ["false-positive.com"],
        target_label="Hard Denied",
        target_bucket="reject",
        source="human",
        path=database,
    )
    record_manual_move(
        ["correct.app"],
        target_label="Investable Candidate",
        target_bucket="priority",
        source="human",
        path=database,
    )

    metrics = performance_metrics(database)

    assert metrics["human_decisions"] == 3
    assert metrics["decisive_comparisons"] == 3
    assert metrics["false_negative_reversals"] == 1
    assert metrics["false_positive_reversals"] == 1
    assert metrics["agreement_rate"] == pytest.approx(1 / 3, abs=0.0001)
    assert metrics["by_extension"][".ai"]["false_negative_reversals"] == 1
    assert metrics["by_extension"][".com"]["false_positive_reversals"] == 1
