#!/usr/bin/env python3
"""Measure how well score_text separates Claude KEEP from Claude REJECT.

The rule scorer decides which candidates are worth a paid Claude call, so its
only job that matters is ranking. This turns "the scoring feels wrong" into a
number: AUC, the chance a randomly chosen KEEP outscores a randomly chosen
REJECT, where 0.5 is a coin flip.

Recorded baselines (253 KEEPs vs 600 REJECTs drawn from the live store):
    0.480  original -- worse than chance; REJECTs averaged a *higher* score
    0.562  same, measured on the full KEEP set rather than a small sample
    0.609  after adding data-derived deep-tech vocabulary
    0.623  after adding the measured boilerplate-density penalty

Reads Claude's own verdicts from the shared store, so it needs a database URL.
Nothing is written. Website text is never committed to the repo -- this always
samples live.

    python scorer_benchmark.py --limit-rejects 600
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def load(limit_rejects: int) -> list[tuple[str, str]]:
    from classification_store import connect_store

    rows: list[tuple[str, str]] = []
    with connect_store(read_only=True) as conn:
        for decision, limit in (("KEEP", 0), ("REJECT", limit_rejects)):
            sql = (
                "SELECT d.decision, c.company_name, c.description, c.excerpt "
                "FROM claude_diligence d JOIN classifications c ON c.domain=d.domain "
                "WHERE d.decision=? AND length("
                "coalesce(c.excerpt,'') || coalesce(c.description,'')) > 120"
            )
            params: list[object] = [decision]
            if limit:
                sql += " LIMIT ?"
                params.append(limit)
            for row in conn.execute(sql, params).fetchall():
                text = " ".join(
                    str(row[key] or "")
                    for key in ("company_name", "description", "excerpt")
                )
                rows.append((str(row["decision"]), text))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-rejects", type=int, default=600)
    args = parser.parse_args()

    import filter as screening_filter

    labelled = load(args.limit_rejects)
    if not labelled:
        print("No labelled Claude decisions available.", file=sys.stderr)
        return 2

    keeps, rejects = [], []
    for decision, text in labelled:
        score, _, _, _ = screening_filter.score_text(text)
        (keeps if decision == "KEEP" else rejects).append(score)

    if not keeps or not rejects:
        print("Need both KEEP and REJECT examples.", file=sys.stderr)
        return 2

    print(f"KEEP   n={len(keeps):4}  mean={statistics.mean(keeps):5.1f}  "
          f"median={statistics.median(keeps):5.1f}")
    print(f"REJECT n={len(rejects):4}  mean={statistics.mean(rejects):5.1f}  "
          f"median={statistics.median(rejects):5.1f}")

    wins = ties = 0
    for k in keeps:
        for r in rejects:
            if k > r:
                wins += 1
            elif k == r:
                ties += 1
    auc = (wins + 0.5 * ties) / (len(keeps) * len(rejects))
    print(f"\nAUC = {auc:.3f}   (0.5 = random)")

    for gate in (20, 40, 60):
        caught = sum(1 for s in keeps if s >= gate)
        admitted = sum(1 for s in rejects if s >= gate)
        print(f"  gate {gate:2}: keeps {100*caught/len(keeps):5.1f}%  "
              f"rejects admitted {100*admitted/len(rejects):5.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
