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

Then on the full labelled set (333 KEEPs vs 2,260 REJECTs, median 15,000
characters of retained page text per row -- see `load`, which now reads the
stored excerpt and homepage copy instead of the one-line description):
    0.614  the same scorer, measured on text it can actually see
    0.648  after zeroing the thesis and business-value components

That last change came from measuring each component on its own rather than
tuning the total: thesis keywords separate KEEP from REJECT at 0.513 and
business value at 0.464 -- one is a coin flip and the other points the wrong
way -- while defensibility manages 0.629 and boilerplate density, inverted,
0.647. Verified on a holdout half (`--holdout`) that the tuning never saw:
0.6020 -> 0.6396 there, so it is not curve-fitting.

Reads Claude's own verdicts from the shared store, so it needs a database URL.
Nothing is written. Website text is never committed to the repo -- this always
samples live.

    python scorer_benchmark.py --limit-rejects 600
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def load(limit_rejects: int) -> list[tuple[str, str]]:
    from classification_store import connect_store

    rows: list[tuple[str, str, str]] = []
    with connect_store(read_only=True) as conn:
        for decision, limit in (("KEEP", 0), ("REJECT", limit_rejects)):
            # record_json carries the retained page copy: title, meta
            # description and homepage excerpt. Without it this measured the
            # scorer against roughly two hundred characters of description --
            # too thin for most of what score_text looks at to appear at all,
            # so whole classes of change came out as exactly no difference.
            sql = (
                "SELECT d.decision, c.domain, c.company_name, c.description, "
                "c.excerpt, c.record_json "
                "FROM claude_diligence d JOIN classifications c ON c.domain=d.domain "
                "WHERE d.decision=?"
            )
            params: list[object] = [decision]
            if limit:
                sql += " LIMIT ?"
                params.append(limit)
            for row in conn.execute(sql, params).fetchall():
                try:
                    record = json.loads(str(row["record_json"] or "{}"))
                except (TypeError, ValueError):
                    record = {}
                text = " ".join(str(value or "") for value in (
                    row["company_name"], row["description"], row["excerpt"],
                    record.get("page_title", ""),
                    record.get("meta_description", ""),
                    record.get("homepage_excerpt", ""),
                ))
                text = " ".join(text.split())
                if len(text) < 200:
                    continue
                rows.append((str(row["decision"]), text, str(row["domain"])))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit-rejects", type=int, default=0)
    parser.add_argument(
        "--holdout", action="store_true",
        help="split by a hash of the domain and report both halves, so a "
             "change can be tuned on one and verified on the other",
    )
    args = parser.parse_args()

    import filter as screening_filter

    labelled = load(args.limit_rejects)
    if not labelled:
        print("No labelled Claude decisions available.", file=sys.stderr)
        return 2

    def halves(rows):
        first, second = [], []
        for row in rows:
            digest = hashlib.sha1(row[2].encode()).hexdigest()
            (first if int(digest, 16) % 2 == 0 else second).append(row)
        return first, second

    def measure(rows):
        keeps, rejects = [], []
        for decision, text, _ in rows:
            score, _, _, _ = screening_filter.score_text(text)
            (keeps if decision == "KEEP" else rejects).append(score)
        return keeps, rejects

    if args.holdout:
        tune, test = halves(labelled)
        for name, subset in (("tune", tune), ("holdout", test)):
            k, r = measure(subset)
            if not k or not r:
                continue
            wins = ties = 0
            for a in k:
                for b in r:
                    if a > b:
                        wins += 1
                    elif a == b:
                        ties += 1
            print(f"{name:<8} n={len(subset):5}  KEEP={len(k):4}  "
                  f"AUC={(wins + 0.5 * ties) / (len(k) * len(r)):.4f}")
        return 0

    keeps, rejects = measure(labelled)

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
