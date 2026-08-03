import boto3
import pytest
from moto import mock_aws

from ai_search.html_archive import append_html, read_html


@pytest.fixture
def s3_bucket(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("S3_SECRET_ACCESS_KEY", raising=False)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


def test_s3_mode_writes_one_object_per_page_and_reads_it_back(s3_bucket, tmp_path):
    pointer = append_html(tmp_path, domain="example.ai", html="<p>hello</p>")

    assert not pointer.path.startswith("/")
    assert pointer.path.startswith("html-archive/")
    assert pointer.offset == 0

    restored = read_html(
        pointer.path, offset=pointer.offset, length=pointer.length, codec=pointer.codec
    )
    assert restored == "<p>hello</p>"


def test_s3_mode_gives_each_page_its_own_object(s3_bucket, tmp_path):
    first = append_html(tmp_path, domain="example.ai", html="<p>one</p>")
    second = append_html(tmp_path, domain="example.ai", html="<p>two</p>")

    assert first.path != second.path
    assert read_html(
        first.path, offset=first.offset, length=first.length, codec=first.codec
    ) == "<p>one</p>"
    assert read_html(
        second.path, offset=second.offset, length=second.length, codec=second.codec
    ) == "<p>two</p>"


def test_pre_migration_local_pointer_still_reads_with_s3_bucket_set(s3_bucket, tmp_path):
    """A pointer written before S3_BUCKET was ever set must keep working.

    Local archive paths are always absolute; only that shape (not a stored
    backend flag) decides how read_html() resolves a pointer, so old local
    rows and new S3 rows can coexist in the same corpus indefinitely.
    """
    import zlib

    local_pointer = tmp_path / "000.htmlpack"
    payload = zlib.compress(b"<p>legacy local page</p>")
    local_pointer.write_bytes(payload)

    restored = read_html(
        str(local_pointer.resolve()), offset=0, length=len(payload), codec="zlib"
    )
    assert restored == "<p>legacy local page</p>"
