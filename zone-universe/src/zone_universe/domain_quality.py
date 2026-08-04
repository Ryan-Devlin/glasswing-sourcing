from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass


VOWELS = frozenset("aeiouy")
KEYBOARD_MASH = (
    "qwerty",
    "asdf",
    "zxcv",
    "qazwsx",
    "poiuy",
    "lkjhg",
    "mnbvc",
)
COMMON_BIGRAMS = frozenset(
    """
    th he in er an re on at en nd ti es or te of ed is it al ar st to
    nt ng se ha as ou io le ve co me de hi ri ro ic ne ea ra ce li ch ll
    be ma si om ur ca el ta la ns di fo ho pe ec pr no us tr wi ac ai
    data tech cloud labs systems software health cyber secure quantum
    """.split()
)


@dataclass(frozen=True)
class DomainQuality:
    accepted: bool
    score: int
    reason: str
    label: str
    length: int
    digit_ratio: float
    vowel_ratio: float
    longest_consonant_run: int
    bigram_ratio: float
    entropy: float

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _longest_consonant_run(value: str) -> int:
    longest = 0
    current = 0
    for character in value:
        if character.isalpha() and character not in VOWELS:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _entropy(value: str) -> float:
    if not value:
        return 0.0
    counts = {character: value.count(character) for character in set(value)}
    total = len(value)
    return -sum(
        (count / total) * math.log2(count / total)
        for count in counts.values()
    )


def _bigram_ratio(letters: str) -> float:
    if len(letters) < 2:
        return 1.0
    bigrams = [
        letters[index : index + 2]
        for index in range(len(letters) - 1)
    ]
    return sum(pair in COMMON_BIGRAMS for pair in bigrams) / len(bigrams)


def assess_domain(domain: str) -> DomainQuality:
    """Conservatively reject obvious generated/machine-noise domain labels.

    This is intentionally not a dictionary-word requirement: short brands,
    acronyms, numbered brands, hyphenated names, and IDNs remain crawlable.
    Only labels with multiple strong randomness signals are excluded.
    """
    normalized = str(domain).strip().lower().strip(".")
    labels = [part for part in normalized.split(".") if part]
    label = labels[-2] if len(labels) >= 2 else (labels[0] if labels else "")
    compact = re.sub(r"[^a-z0-9]", "", label)
    letters = "".join(character for character in compact if character.isalpha())
    digits = "".join(character for character in compact if character.isdigit())
    length = len(compact)
    digit_ratio = len(digits) / max(length, 1)
    vowel_ratio = (
        sum(character in VOWELS for character in letters)
        / max(len(letters), 1)
    )
    consonant_run = _longest_consonant_run(letters)
    bigram_ratio = _bigram_ratio(letters)
    entropy = _entropy(compact)
    hyphen_count = label.count("-")
    label_parts = [
        part for part in label.split("-") if part
    ]

    def result(accepted: bool, score: int, reason: str) -> DomainQuality:
        return DomainQuality(
            accepted=accepted,
            score=max(0, min(int(score), 100)),
            reason=reason,
            label=label,
            length=length,
            digit_ratio=round(digit_ratio, 4),
            vowel_ratio=round(vowel_ratio, 4),
            longest_consonant_run=consonant_run,
            bigram_ratio=round(bigram_ratio, 4),
            entropy=round(entropy, 4),
        )

    if not compact:
        return result(False, 100, "empty_domain_label")
    if label.startswith("xn--"):
        return result(True, 0, "internationalized_domain")
    if any(pattern in compact for pattern in KEYBOARD_MASH):
        return result(False, 100, "keyboard_mash")
    if re.search(r"(.)\1{5,}", compact):
        return result(False, 100, "excessive_character_repetition")
    if (
        hyphen_count >= 8
        or (
            hyphen_count >= 5
            and hyphen_count / max(len(label), 1) >= 0.35
        )
    ):
        return result(False, 99, "excessive_hyphenation")
    if (
        len(label_parts) >= 4
        and len(set(label_parts)) == 1
    ):
        return result(False, 99, "repeated_domain_segment")
    periodic = re.fullmatch(r"(.{1,4})\1{3,}", compact)
    if periodic and length >= 12:
        return result(False, 98, "repeated_character_pattern")
    if re.fullmatch(r"0x[a-f0-9]{1,10}", compact):
        return result(True, 0, "hex_brand_safe")
    if (
        compact.isdigit()
        and (
            length >= 4
            or (length >= 3 and compact.startswith("0"))
        )
    ):
        return result(False, 98, "long_numeric_label")
    if (
        length >= 4
        and digit_ratio >= 0.6
        and len(letters) <= 2
    ):
        return result(False, 95, "numeric_dominant_generated_label")
    if (
        (
            (length >= 8 and re.match(r"^\d{3,}", compact))
            or (length >= 6 and re.match(r"^0{2,}\d*", compact))
        )
        and digit_ratio >= 0.5
        and (len(letters) <= 2 or bigram_ratio < 0.2)
    ):
        return result(False, 92, "generated_numeric_prefix_label")
    if length <= 10:
        return result(True, 0, "short_brand_safe")
    if (
        11 <= length <= 17
        and consonant_run >= 7
        and vowel_ratio < 0.12
        and bigram_ratio < 0.08
    ):
        return result(False, 90, "compact_consonant_dga")
    if (
        length >= 14
        and len(digits) >= 5
        and (
            digit_ratio >= 0.28
            or (length >= 20 and digit_ratio >= 0.18 and entropy >= 3.5)
        )
        and len(set(compact)) >= 8
    ):
        return result(False, 97, "generated_alphanumeric_label")

    signals = 0
    reasons: list[str] = []
    if length >= 18 and consonant_run >= 6:
        signals += 2
        reasons.append("long_consonant_run")
    if length >= 18 and (vowel_ratio < 0.16 or vowel_ratio > 0.72):
        signals += 2
        reasons.append("implausible_vowel_balance")
    if length >= 18 and bigram_ratio < 0.13:
        signals += 2
        reasons.append("low_language_bigram_ratio")
    if length >= 22 and entropy >= 3.65:
        signals += 1
        reasons.append("high_character_entropy")
    if length >= 20 and len(set(compact)) / length >= 0.72:
        signals += 1
        reasons.append("high_unique_character_ratio")
    if len(digits) >= 3 and length >= 16:
        signals += 1
        reasons.append("embedded_digit_noise")

    if signals >= 4:
        return result(
            False,
            min(96, 70 + signals * 5),
            "+".join(reasons),
        )
    return result(
        True,
        min(60, signals * 12),
        "+".join(reasons) or "plausible_domain_label",
    )


COMPANY_HINTS = (
    "ai",
    "analytics",
    "api",
    "automation",
    "bio",
    "biotech",
    "cloud",
    "compute",
    "cyber",
    "data",
    "defense",
    "fintech",
    "genai",
    "health",
    "healthtech",
    "labs",
    "llm",
    "medical",
    "ml",
    "neural",
    "nlp",
    "platform",
    "quantum",
    "robot",
    "robotics",
    "saas",
    "security",
    "software",
    "systems",
    "tech",
    "vision",
)

# .app is a small, inherently startup-signaling universe (registration
# requires HTTPS, and it's marketed toward developers/mobile apps) --
# nowhere near the scale of .com, where the same permissive "plausible
# word" tier let through the entire long tail of small-business and
# personal-site domains alongside real companies. Keep .app's broader
# recall; apply the stricter, keyword/short-brand-only policy to .com.
PERMISSIVE_TLDS = frozenset({"app"})


def crawl_priority(domain: str) -> int:
    """Rank accepted names for crawl order by thesis-relevant signal.

    0 is the first pass. Priority 3 stays in the source queue, recoverable
    later, but isn't actively worked -- a domain merely "not being
    gibberish" isn't evidence of being an operating tech company (a plain
    dictionary word like "flowers.com" and an invented brand like
    "figma.com" are statistically identical letter sequences), so it takes
    an explicit thesis keyword, a short/brandable name, or a distinctive
    digit-stylized brand (1Password, 3D Scanner) to earn an active tier.
    """
    quality = assess_domain(domain)
    label = quality.label.removeprefix("xn--")
    tld = str(domain).strip().lower().strip(".").rsplit(".", 1)[-1]
    letters_only = label.isalpha()
    company_hint = any(
        hint in label
        for hint in COMPANY_HINTS
        if hint != "ai"
    ) or (
        label == "ai"
        or label.startswith("ai-")
        or label.endswith("-ai")
        or label.endswith("ai")
    )
    if (
        company_hint
        and quality.digit_ratio == 0
        and 3 <= quality.length <= 32
    ):
        return 0
    if (
        letters_only
        and 1 <= quality.length <= 5
    ):
        return 1
    if (
        2 <= quality.length <= 5
        and bool(re.search(r"[a-z]", label))
        and quality.digit_ratio <= 0.5
    ):
        return 1
    if quality.reason == "hex_brand_safe":
        return 1
    if (
        not letters_only
        and quality.length >= 14
        and quality.score > 0
    ):
        return 3
    # Compact names that mix digits into an otherwise plausible brand word
    # (1Password, 3D Scanner-style stylization) -- the digit is the brand
    # signal here, not a random-generation artifact, so this stays narrow
    # (short, low digit ratio, low noise score) rather than reopening the
    # door to generated-looking alphanumeric strings.
    if (
        not letters_only
        and quality.length <= 12
        and quality.digit_ratio <= 0.3
        and quality.score <= 20
    ):
        return 1
    if (
        tld in PERMISSIVE_TLDS
        and letters_only
        and 4 <= quality.length <= 15
        and 0.18 <= quality.vowel_ratio <= 0.72
        and quality.bigram_ratio >= 0.20
        and quality.longest_consonant_run <= 5
    ):
        return 1
    if tld in PERMISSIVE_TLDS and letters_only and 2 <= quality.length <= 24:
        return 2
    if (
        tld in PERMISSIVE_TLDS
        and quality.score <= 24
        and quality.digit_ratio < 0.25
        and quality.length <= 24
    ):
        return 2
    return 3
