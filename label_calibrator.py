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
from collections import Counter, defaultdict
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
    """Weighted multinomial model using only the stable training split."""

    def __init__(self, rows: list[dict[str, str]]) -> None:
        usable = [
            row for row in rows
            if row.get("split") == "train"
            and row.get("label") in {"yes", "no"}
            and row.get("trust_tier") in {"gold", "silver"}
            and not unusable_training_text(row.get("input_text", ""))
        ]
        self.document_counts = Counter(row["label"] for row in usable)
        self.feature_counts: dict[str, Counter[str]] = {
            "yes": Counter(),
            "no": Counter(),
        }
        document_frequency: Counter[str] = Counter()
        for row in usable:
            label = row["label"]
            features = text_features(row.get("input_text", ""))
            document_frequency.update(features)
            # Human and verified-portfolio labels are authoritative. Silver
            # labels inform the negative class, but at half strength.
            trust_weight = 1.0 if row.get("trust_tier") == "gold" else 0.5
            sample_weight = float(row.get("sample_weight") or 1.0)
            weight = trust_weight * sample_weight
            for feature, count in features.items():
                self.feature_counts[label][feature] += count * weight

        minimum_documents = max(2, round(len(usable) * 0.015))
        self.vocabulary = {
            feature
            for feature, count in document_frequency.items()
            if count >= minimum_documents
        }
        self.totals = {
            label: sum(
                count for feature, count in counts.items()
                if feature in self.vocabulary
            )
            for label, counts in self.feature_counts.items()
        }

    def predict(self, text: str) -> Calibration:
        features = text_features(text)
        vocabulary_size = max(len(self.vocabulary), 1)
        class_scores: dict[str, float] = {}
        contributions: list[tuple[float, str]] = []
        matched_weight = sum(
            min(count, 2)
            for feature, count in features.items()
            if feature in self.vocabulary
        )

        # Balanced priors make the score reflect language rather than the
        # current number of Yes and No labels.
        for label in ("yes", "no"):
            denominator = self.totals[label] + vocabulary_size
            score = math.log(0.5)
            for feature, count in features.items():
                if feature not in self.vocabulary:
                    continue
                probability = (
                    self.feature_counts[label].get(feature, 0.0) + 1.0
                ) / denominator
                score += min(count, 2) * math.log(probability)
            class_scores[label] = score

        # Website pages contain many correlated words. Normalizing by the
        # square root of matched evidence prevents long pages from producing
        # fake 100% certainty while retaining a useful directional margin.
        raw_margin = class_scores["yes"] - class_scores["no"]
        margin = raw_margin / max(math.sqrt(matched_weight), 1.0)
        margin = max(-12.0, min(12.0, margin))
        probability_yes = 1.0 / (1.0 + math.exp(-margin))
        for feature in features:
            if feature not in self.vocabulary:
                continue
            yes_probability = (
                self.feature_counts["yes"].get(feature, 0.0) + 1.0
            ) / (self.totals["yes"] + vocabulary_size)
            no_probability = (
                self.feature_counts["no"].get(feature, 0.0) + 1.0
            ) / (self.totals["no"] + vocabulary_size)
            contribution = math.log(yes_probability / no_probability)
            contributions.append((contribution, feature))

        label = "yes" if probability_yes >= 0.5 else "no"
        ranked = sorted(
            contributions,
            key=lambda item: item[0],
            reverse=label == "yes",
        )
        return Calibration(
            label=label,
            probability_yes=round(probability_yes, 6),
            confidence=round(abs(probability_yes - 0.5) * 2, 6),
            evidence=tuple(feature for _, feature in ranked[:8]),
            # A heterogeneous investment thesis cannot be generalized safely
            # from eleven usable positive training websites. Keep collecting
            # labels and activate learned routing only at a defensible floor.
            training_ready=(
                self.document_counts["yes"] >= 30
                and self.document_counts["no"] >= 30
            ),
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
