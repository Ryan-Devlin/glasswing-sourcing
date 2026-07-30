from __future__ import annotations

import hashlib
import zlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ArchivePointer:
    path: str
    offset: int
    length: int
    codec: str
    raw_bytes: int


def shard_for_domain(domain: str, shard_count: int = 64) -> int:
    digest = hashlib.blake2b(domain.encode("utf-8"), digest_size=2).digest()
    return int.from_bytes(digest, "big") % shard_count


def append_html(
    root: Path,
    *,
    domain: str,
    tld: str,
    html: str,
) -> ArchivePointer:
    """Append one compressed HTML bundle and return its durable byte pointer."""
    raw = html.encode("utf-8", errors="replace")
    compressed = zlib.compress(raw, level=6)
    shard = shard_for_domain(domain)
    path = root / tld / f"{shard:03d}.htmlpack"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as handle:
        offset = handle.tell()
        handle.write(compressed)
        handle.flush()
    return ArchivePointer(
        path=str(path.resolve()),
        offset=offset,
        length=len(compressed),
        codec="zlib",
        raw_bytes=len(raw),
    )


def read_html(
    path: Path | str,
    *,
    offset: int,
    length: int,
    codec: str,
) -> str:
    archive = Path(path)
    if not archive.exists() or length <= 0:
        return ""
    with archive.open("rb") as handle:
        handle.seek(max(0, int(offset)))
        payload = handle.read(max(0, int(length)))
    if len(payload) != int(length):
        return ""
    if codec == "zlib":
        payload = zlib.decompress(payload)
    return payload.decode("utf-8", errors="replace")
