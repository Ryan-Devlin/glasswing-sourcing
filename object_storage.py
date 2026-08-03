"""Dual object-storage backend: local filesystem by default, S3-compatible
(Cloudflare R2, AWS S3, MinIO, ...) in the cloud.

Selected by the presence of `S3_BUCKET`, mirroring db_backend.py's approach
to the database backend: absent it, everything reads/writes local files with
zero extra setup (always able to run locally); set it, and the same calls
go to whichever S3-compatible endpoint `S3_ENDPOINT_URL` points at, so
switching providers or accounts is a config change, not a code change.

Only for objects that are written once and read whole (manual snapshots).
The HTML archive format (sharded, append-only files addressed by byte
offset+length) is a fundamentally different shape that doesn't map onto
S3's immutable-object model without a storage-format redesign -- see
CLOUD_MIGRATION.md.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def using_object_storage() -> bool:
    return bool(os.getenv("S3_BUCKET", "").strip())


def _clean_env(name: str) -> str | None:
    # A trailing newline/space from copy-pasting a credential into a
    # dashboard's variable field produces a technically-different string
    # that still *looks* right at a glance -- but R2/S3 request signing is
    # byte-exact, so it fails with an opaque "signature does not match"
    # rather than an obviously-wrong-credential error. Stripped once here,
    # for every credential this module reads.
    value = os.getenv(name)
    return value.strip() or None if value else None


def _client():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=_clean_env("S3_ENDPOINT_URL"),
        aws_access_key_id=_clean_env("S3_ACCESS_KEY_ID"),
        aws_secret_access_key=_clean_env("S3_SECRET_ACCESS_KEY"),
        region_name=_clean_env("S3_REGION") or "auto",
    )


def write_bytes(key: str, data: bytes) -> None:
    """S3 PutObject is already atomic (whole-object replace); the local
    branch uses write-then-rename so a crash mid-write can never leave a
    truncated snapshot file in place of a good one.
    """
    if using_object_storage():
        _client().put_object(
            Bucket=os.environ["S3_BUCKET"], Key=key, Body=data
        )
        return
    path = Path(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = ""
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        temporary_name = ""
    finally:
        if temporary_name:
            Path(temporary_name).unlink(missing_ok=True)


def read_bytes(key: str) -> bytes:
    if using_object_storage():
        response = _client().get_object(Bucket=os.environ["S3_BUCKET"], Key=key)
        return response["Body"].read()
    return Path(key).read_bytes()


def exists(key: str) -> bool:
    if using_object_storage():
        import botocore

        try:
            _client().head_object(Bucket=os.environ["S3_BUCKET"], Key=key)
            return True
        except botocore.exceptions.ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return False
            raise
    return Path(key).exists()


def list_keys(prefix: str, *, suffix: str = "") -> list[str]:
    """List keys/paths under a prefix, optionally filtered by suffix.

    `prefix` is an S3 key prefix in object-storage mode, or a local
    directory path otherwise -- matching how callers already pass
    `snapshot_root` either way.
    """
    if using_object_storage():
        client = _client()
        keys: list[str] = []
        continuation: dict[str, str] = {}
        while True:
            response = client.list_objects_v2(
                Bucket=os.environ["S3_BUCKET"], Prefix=prefix, **continuation
            )
            keys.extend(
                obj["Key"]
                for obj in response.get("Contents", [])
                if not suffix or obj["Key"].endswith(suffix)
            )
            if not response.get("IsTruncated"):
                break
            continuation = {"ContinuationToken": response["NextContinuationToken"]}
        return keys
    root = Path(prefix)
    if not root.exists():
        return []
    return [str(path) for path in root.glob(f"*{suffix}")]
