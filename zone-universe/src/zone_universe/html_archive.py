from __future__ import annotations

import hashlib
import uuid
import zlib
from dataclasses import dataclass
from pathlib import Path

from object_storage import read_bytes as _s3_read_bytes
from object_storage import using_object_storage
from object_storage import write_bytes as _s3_write_bytes

OBJECT_STORAGE_ARCHIVE_PREFIX = "html-archive"


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


def _is_object_storage_key(path: str) -> bool:
    # Local archive paths are always written as str(Path.resolve()), which on
    # macOS/Linux is always absolute (leading '/'); S3 keys are deliberately
    # written without one. This lets read_html() tell old, already-written
    # local pointers apart from new S3 ones without any schema/flag column,
    # so switching S3_BUCKET on mid-corpus never breaks pointers written
    # before the switch.
    return not path.startswith("/")


def append_html(
    root: Path,
    *,
    domain: str,
    tld: str,
    html: str,
) -> ArchivePointer:
    """Append one compressed HTML bundle and return its durable byte pointer.

    Local mode appends into a byte-addressed sharded file, unchanged.
    Object-storage mode (S3_BUCKET set) instead writes one object per page --
    S3 has no equivalent to appending into an existing object, and one PUT
    per page is the natural fit for its immutable-object model.
    """
    raw = html.encode("utf-8", errors="replace")
    compressed = zlib.compress(raw, level=6)
    shard = shard_for_domain(domain)
    if using_object_storage():
        key = f"{OBJECT_STORAGE_ARCHIVE_PREFIX}/{tld}/{shard:03d}/{uuid.uuid4().hex}.zlib"
        _s3_write_bytes(key, compressed)
        return ArchivePointer(
            path=key,
            offset=0,
            length=len(compressed),
            codec="zlib",
            raw_bytes=len(raw),
        )
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
    path = str(path)
    if length <= 0:
        return ""
    if _is_object_storage_key(path):
        payload = _s3_read_bytes(path)
    else:
        archive = Path(path)
        if not archive.exists():
            return ""
        with archive.open("rb") as handle:
            handle.seek(max(0, int(offset)))
            payload = handle.read(max(0, int(length)))
        if len(payload) != int(length):
            return ""
    if codec == "zlib":
        payload = zlib.decompress(payload)
    return payload.decode("utf-8", errors="replace")
