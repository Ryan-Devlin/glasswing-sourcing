from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit


def _normalize_domain(value: object, *, require_ai: bool) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    candidate = text if "://" in text else f"//{text}"
    parsed = urlsplit(candidate)
    host = (parsed.hostname or "").strip(".")
    if host.startswith("www."):
        host = host[4:]
    try:
        ipaddress.ip_address(host)
        return ""
    except ValueError:
        pass
    labels = host.split(".")
    if len(labels) < 2 or (require_ai and labels[-1] != "ai"):
        return ""
    if any(
        not label
        or len(label) > 63
        or label.startswith("-")
        or label.endswith("-")
        or not all(ch.isalnum() or ch == "-" for ch in label)
        for label in labels
    ):
        return ""
    return host


def normalize_domain(value: object) -> str:
    """Normalize a domain discovered by the dedicated .ai source."""
    return _normalize_domain(value, require_ai=True)


def normalize_any_domain(value: object) -> str:
    """Normalize a manually supplied domain from the source-agnostic intake."""
    return _normalize_domain(value, require_ai=False)
