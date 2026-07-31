import boto3
import pytest
from moto import mock_aws

import object_storage


def test_using_object_storage_reflects_s3_bucket(monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    assert object_storage.using_object_storage() is False
    monkeypatch.setenv("S3_BUCKET", "some-bucket")
    assert object_storage.using_object_storage() is True


def test_local_write_read_roundtrip_is_atomic(tmp_path, monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    key = str(tmp_path / "nested" / "file.json")

    object_storage.write_bytes(key, b'{"a":1}')

    assert object_storage.exists(key) is True
    assert object_storage.read_bytes(key) == b'{"a":1}'
    # No leftover temp file from the write-then-rename dance.
    leftovers = list((tmp_path / "nested").glob(".*"))
    assert leftovers == []


def test_local_list_keys_filters_by_suffix(tmp_path, monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    object_storage.write_bytes(str(tmp_path / "a.json"), b"{}")
    object_storage.write_bytes(str(tmp_path / "a.json.sha256"), b"abc")
    object_storage.write_bytes(str(tmp_path / "b.txt"), b"nope")

    found = object_storage.list_keys(str(tmp_path), suffix=".json")
    assert found == [str(tmp_path / "a.json")]


@pytest.fixture
def s3_bucket(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("S3_SECRET_ACCESS_KEY", raising=False)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(
            Bucket="test-bucket"
        )
        yield


def test_s3_write_read_roundtrip(s3_bucket):
    object_storage.write_bytes("snapshots/a.json", b'{"a":1}')
    assert object_storage.exists("snapshots/a.json") is True
    assert object_storage.exists("snapshots/missing.json") is False
    assert object_storage.read_bytes("snapshots/a.json") == b'{"a":1}'


def test_s3_list_keys_filters_by_suffix_and_prefix(s3_bucket):
    object_storage.write_bytes("snapshots/a.json", b"{}")
    object_storage.write_bytes("snapshots/a.json.sha256", b"abc")
    object_storage.write_bytes("other/b.json", b"{}")

    found = object_storage.list_keys("snapshots", suffix=".json")
    assert found == ["snapshots/a.json"]
