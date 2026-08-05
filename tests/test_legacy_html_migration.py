from __future__ import annotations

import hashlib
import zlib
from pathlib import Path

import pytest

from migrate_legacy_html_to_object_storage import (
    is_object_key,
    raw_payload_identity,
    read_legacy_block,
    resolve_legacy_path,
)


def test_resolve_legacy_path_maps_old_machine_root(tmp_path: Path):
    archive = tmp_path / "html"
    archive.mkdir()
    pack = archive / "007.htmlpack"
    pack.write_bytes(b"payload")
    old = "/Users/someone/project/data/html/007.htmlpack"
    assert resolve_legacy_path(old, archive) == pack


def test_read_legacy_block_verifies_compressed_payload(tmp_path: Path):
    archive = tmp_path / "html"
    archive.mkdir()
    raw = b"<html>valuable company</html>"
    payload = zlib.compress(raw)
    pack = archive / "001.htmlpack"
    pack.write_bytes(b"junk" + payload + b"tail")
    assert read_legacy_block(
        pack,
        archive_root=archive,
        offset=4,
        length=len(payload),
        codec="zlib",
        raw_hash=hashlib.sha256(raw).hexdigest(),
    ) == payload


def test_read_legacy_block_rejects_bad_hash(tmp_path: Path):
    archive = tmp_path / "html"
    archive.mkdir()
    pack = archive / "001.htmlpack"
    payload = zlib.compress(b"wrong")
    pack.write_bytes(payload)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        read_legacy_block(
            pack,
            archive_root=archive,
            offset=0,
            length=len(payload),
            codec="zlib",
            raw_hash="0" * 64,
        )


def test_object_key_detection():
    assert is_object_key("html-archive/migrated/ai/a/a.zlib") is True
    assert is_object_key("/data/html/001.htmlpack") is False


def test_raw_payload_identity_reconstructs_legacy_missing_hash():
    raw = b"<html>legacy page without database hash</html>"
    assert raw_payload_identity(zlib.compress(raw), "zlib") == (
        hashlib.sha256(raw).hexdigest(),
        len(raw),
    )
