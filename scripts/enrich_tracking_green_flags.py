#!/usr/bin/env python3
"""Persist source-backed green flags for the current tracked-company set."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from classification_store import connect_store


FLAGS = {
    "kaiaai.ai": [
        {"type": "ex_big_tech", "label": "Enterprise operator", "evidence": "Founder Rahul Gupta spent eight years as CTO at Conduent managing 5,000 engineers, then served as CEO of Technology Services at Toptal.", "source": "https://kaiaai.ai/about"},
    ],
    "3lc.ai": [
        {"type": "repeat_founder", "label": "Serial founders", "evidence": "3LC says its four-person founding team worked together for more than a decade across three companies; Paul Endresen and Thomas Hagen previously co-founded Innerloop Studios and Hue.", "source": "https://3lc.ai/about/"},
        {"type": "enterprise_traction", "label": "Enterprise production", "evidence": "3LC reports production deployments across four Equinor teams and names energy, aerospace, agriculture, and industrial-AI customers.", "source": "https://3lc.ai/"},
        {"type": "strategic_backing", "label": "Strategic backing", "evidence": "3LC disclosed a $1.8M seed round backed by investors including EV Private Equity and Shell Ventures.", "source": "https://3lc.ai/3lc-inc-secures-1-8-million-in-seed-funding-to-revolutionize-how-machine-learning-models-are-trained/"},
    ],
    "neuramorphic.ai": [
        {"type": "deep_tech_ip", "label": "Patent-pending IP", "evidence": "Neuramorphic reports a U.S. provisional patent application covering its neuromorphic edge-inference architecture and custom CUDA runtime.", "source": "https://neuramorphic.ai/about"},
        {"type": "strategic_backing", "label": "NVIDIA Inception", "evidence": "Neuramorphic identifies itself as an NVIDIA Inception member and AWS Activate participant.", "source": "https://neuramorphic.ai/about"},
    ],
    "metaneural.ai": [
        {"type": "top_accelerator", "label": "Accelerator-backed", "evidence": "MetaNeural was publicly announced as joining the International Accelerator program.", "source": "https://www.internationalaccelerator.com/post/metaneural-joins-the-international-accelerator"},
    ],
    "inceptioncyber.ai": [
        {"type": "ex_big_tech", "label": "Ex-Cisco & Microsoft", "evidence": "Founder and CTO Abhishek Singh previously worked at Cisco, FireEye, and Microsoft.", "source": "https://inceptioncyber.ai/about-us"},
        {"type": "deep_tech_ip", "label": "41 security patents", "evidence": "Inception Cyber states that founder Abhishek Singh holds 41 patents in AI and cybersecurity.", "source": "https://inceptioncyber.ai/about-us"},
        {"type": "repeat_founder", "label": "Repeat operator", "evidence": "Co-founder Bill Mann previously served as CEO of Styra and held product and security leadership roles at Centrify and CA Technologies.", "source": "https://inceptioncyber.ai/about-us"},
        {"type": "enterprise_traction", "label": "Live enterprise usage", "evidence": "Inception Cyber reports live deployments analyzing tens of millions of emails.", "source": "https://inceptioncyber.ai/"},
    ],
    "deepen.ai": [
        {"type": "ex_big_tech", "label": "Ex-Google founders", "evidence": "Founder Mohammad Musa and co-founder Anil Muthineni are reported as former Google employees; Muthineni also worked at Microsoft.", "source": "https://wellfound.com/company/deepen-ai/people"},
        {"type": "top_university_spinout", "label": "IIT Kanpur team", "evidence": "Co-founder Anil Muthineni is identified as an IIT Kanpur graduate.", "source": "https://wellfound.com/company/deepen-ai/people"},
    ],
    "yunaverse.app": [
        {"type": "deep_tech_ip", "label": "Patent portfolio", "evidence": "Yunaverse reports eight USPTO provisional patent applications, more than 400 claims, and five peer-reviewed publications.", "source": "https://yunaverse.app/"},
    ],
}


def main() -> int:
    changed = 0
    with connect_store() as conn:
        for domain, flags in FLAGS.items():
            tracked = conn.execute(
                "SELECT 1 FROM tracked_domains WHERE domain=?", (domain,)
            ).fetchone()
            row = conn.execute(
                "SELECT record_json FROM classifications WHERE domain=?", (domain,)
            ).fetchone()
            if not tracked or not row:
                continue
            try:
                record = json.loads(str(row["record_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                record = {}
            record["website_green_flags"] = flags
            conn.execute(
                "UPDATE classifications SET record_json=? WHERE domain=?",
                (json.dumps(record, ensure_ascii=False, separators=(",", ":")), domain),
            )
            changed += 1
        conn.commit()
    print(f"Updated {changed} tracked companies with verified green flags.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
