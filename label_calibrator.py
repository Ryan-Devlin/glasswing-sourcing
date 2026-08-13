"""Local text calibration learned from Ryan's trusted Yes/No decisions.

The rules in ``filter.py`` establish factual exclusions and extract structured
signals.  This module supplies the missing preference layer: it learns which
otherwise plausible companies resemble prior Yes versus No decisions without
calling an API or memorizing a list of domains.
"""

from __future__ import annotations

import csv
import math
import re
from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_TRAINING_DATA = ROOT / "data" / "training" / "glasswing_model_ready.csv"
TOKEN_RE = re.compile(r"[a-z][a-z0-9+#.-]{1,30}")
STOPWORDS = {
    "about", "also", "and", "are", "been", "being", "but", "can", "company",
    "for", "from", "has", "have", "into", "its", "more", "our", "that",
    "the", "their", "they", "this", "through", "using", "website", "with",
    "your", "nan",
}


def unusable_training_text(text: str) -> bool:
    normalized = str(text).lower()
    return not normalized.strip() or len(re.findall(r"\bnan\b", normalized)) >= 3


def text_features(text: str) -> Counter[str]:
    """Return capped unigram/bigram counts for compact website evidence."""
    tokens = [
        token.strip(".-")
        for token in TOKEN_RE.findall(str(text).lower())
        if token not in STOPWORDS
    ]
    features: Counter[str] = Counter(tokens)
    features.update(
        f"{left}_{right}"
        for left, right in zip(tokens, tokens[1:])
        if left not in STOPWORDS and right not in STOPWORDS
    )
    return Counter({key: min(value, 3) for key, value in features.items()})


@dataclass(frozen=True)
class Calibration:
    label: str
    probability_yes: float
    confidence: float
    evidence: tuple[str, ...]
    training_ready: bool


class PreferenceCalibrator:
    """Human-only TF-IDF centroid model using a stable domain split.

    Class centroids compare semantic language without letting the larger No
    class or verbose pages dominate. Claude labels never enter this dataset.
    """

    def __init__(self, rows: list[dict[str, str]]) -> None:
        usable = [
            row for row in rows
            if row.get("split") == "train"
            and row.get("label") in {"yes", "no"}
            and row.get("trust_tier") in {"gold", "silver"}
            and not unusable_training_text(row.get("input_text", ""))
        ]
        self.document_counts = Counter(row["label"] for row in usable)
        self.rows = usable
        document_frequency: Counter[str] = Counter()
        for row in usable:
            features = text_features(row.get("input_text", ""))
            document_frequency.update(features.keys())

        minimum_documents = 1 if len(usable) < 20 else max(
            2, round(len(usable) * 0.015)
        )
        self.vocabulary = {
            feature
            for feature, count in document_frequency.items()
            if count >= minimum_documents
        }
        total_documents = max(len(usable), 1)
        self.idf = {
            feature: math.log((1 + total_documents) / (1 + count)) + 1
            for feature, count in document_frequency.items()
            if feature in self.vocabulary
        }
        self.centroids: dict[str, Counter[str]] = {
            "yes": Counter(), "no": Counter(),
        }
        class_weights = Counter()
        for row in usable:
            label = row["label"]
            trust_weight = 1.0 if row.get("trust_tier") == "gold" else 0.5
            sample_weight = float(row.get("sample_weight") or 1.0)
            weight = trust_weight * sample_weight
            vector = self._vector(row.get("input_text", ""))
            for feature, value in vector.items():
                self.centroids[label][feature] += value * weight
            class_weights[label] += weight
        for label in ("yes", "no"):
            divisor = max(float(class_weights[label]), 1.0)
            self.centroids[label] = Counter({
                feature: value / divisor
                for feature, value in self.centroids[label].items()
            })
        self.decision_threshold = self._validation_threshold(rows)
        self.validation_metrics = self._evaluate(
            [row for row in rows if row.get("split") == "validation"]
        )
        self.training_ready = bool(
            self.document_counts["yes"] >= 50
            and self.document_counts["no"] >= 50
            and self.validation_metrics["auc"] >= 0.70
            and self.validation_metrics["positive_recall"] >= 0.60
        )

    def _vector(self, text: str) -> Counter[str]:
        counts = text_features(text)
        vector = Counter({
            feature: (1 + math.log(count)) * self.idf[feature]
            for feature, count in counts.items()
            if feature in self.idf
        })
        norm = math.sqrt(sum(value * value for value in vector.values()))
        if norm:
            return Counter({key: value / norm for key, value in vector.items()})
        return vector

    @staticmethod
    def _cosine(vector: Counter[str], centroid: Counter[str]) -> float:
        denominator = math.sqrt(sum(value * value for value in centroid.values()))
        if not vector or not denominator:
            return 0.0
        return sum(value * centroid.get(key, 0.0) for key, value in vector.items()) / denominator

    def _raw_probability(self, text: str) -> float:
        vector = self._vector(text)
        margin = (
            self._cosine(vector, self.centroids["yes"])
            - self._cosine(vector, self.centroids["no"])
        )
        return 1.0 / (1.0 + math.exp(-8.0 * margin))

    @staticmethod
    def _auc(values: list[tuple[float, str]]) -> float:
        yes = [score for score, label in values if label == "yes"]
        no = [score for score, label in values if label == "no"]
        if not yes or not no:
            return 0.0
        wins = sum(left > right for left in yes for right in no)
        ties = sum(left == right for left in yes for right in no)
        return (wins + 0.5 * ties) / (len(yes) * len(no))

    def _validation_threshold(self, rows: list[dict[str, str]]) -> float:
        values = [
            (self._raw_probability(row.get("input_text", "")), row["label"])
            for row in rows
            if row.get("split") == "validation" and row.get("label") in {"yes", "no"}
        ]
        positives = sum(label == "yes" for _, label in values)
        negatives = sum(label == "no" for _, label in values)
        if not positives or not negatives:
            return 0.5
        candidates = sorted({score for score, _ in values} | {0.5})
        def quality(threshold: float) -> tuple[float, float]:
            recall = sum(score >= threshold and label == "yes" for score, label in values) / positives
            specificity = sum(score < threshold and label == "no" for score, label in values) / negatives
            return ((recall + specificity) / 2, -abs(threshold - 0.5))
        return max(candidates, key=quality)

    def _evaluate(self, rows: list[dict[str, str]]) -> dict[str, float]:
        values = [
            (self._raw_probability(row.get("input_text", "")), row["label"])
            for row in rows
            if row.get("label") in {"yes", "no"}
        ]
        positives = sum(label == "yes" for _, label in values)
        negatives = sum(label == "no" for _, label in values)
        return {
            "auc": round(self._auc(values), 6),
            "positive_recall": round(
                sum(score >= self.decision_threshold and label == "yes" for score, label in values)
                / max(positives, 1),
                6,
            ),
            "specificity": round(
                sum(score < self.decision_threshold and label == "no" for score, label in values)
                / max(negatives, 1),
                6,
            ),
        }

    def predict(self, text: str) -> Calibration:
        vector = self._vector(text)
        yes_similarity = self._cosine(vector, self.centroids["yes"])
        no_similarity = self._cosine(vector, self.centroids["no"])
        margin = yes_similarity - no_similarity
        # Similarities are intentionally modest; scale their difference into
        # a useful probability while retaining uncertainty near 0.5.
        probability_yes = 1.0 / (1.0 + math.exp(-8.0 * margin))
        contributions = [
            (
                value * (
                    self.centroids["yes"].get(feature, 0.0)
                    - self.centroids["no"].get(feature, 0.0)
                ),
                feature,
            )
            for feature, value in vector.items()
        ]

        label = "yes" if probability_yes >= self.decision_threshold else "no"
        ranked = sorted(
            contributions,
            key=lambda item: item[0],
            reverse=label == "yes",
        )
        return Calibration(
            label=label,
            probability_yes=round(probability_yes, 6),
            confidence=round(
                min(1.0, abs(probability_yes - self.decision_threshold) * 4),
                6,
            ),
            evidence=tuple(feature for _, feature in ranked[:8]),
            # A heterogeneous investment thesis cannot be generalized safely
            # from eleven usable positive training websites. Keep collecting
            # labels and activate learned routing only at a defensible floor.
            training_ready=self.training_ready,
        )


def read_training_rows(path: Path = DEFAULT_TRAINING_DATA) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


@lru_cache(maxsize=4)
def load_calibrator(
    path: str = str(DEFAULT_TRAINING_DATA),
    modified_ns: int | None = None,
) -> PreferenceCalibrator | None:
    training_path = Path(path)
    if not training_path.exists():
        return None
    rows = read_training_rows(training_path)
    if not rows:
        return None
    return PreferenceCalibrator(rows)


def calibrate_text(
    text: str,
    path: Path = DEFAULT_TRAINING_DATA,
) -> Calibration | None:
    """Score text, automatically refreshing when the training CSV changes."""
    if not path.exists():
        return None
    model = load_calibrator(str(path), path.stat().st_mtime_ns)
    return model.predict(text) if model is not None else None
