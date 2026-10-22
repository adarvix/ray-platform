"""Filesystem helpers so the same code runs against local disk, any S3-compatible store and S3.

Credentials are never passed in code. On EKS the AWS SDK picks up the IRSA
web-identity token mounted into the pod; on a local cluster, the standard
AWS_* environment variables point at SeaweedFS.
"""
from __future__ import annotations

import os
from functools import lru_cache

import pyarrow.fs as pafs


@lru_cache(maxsize=None)
def _s3() -> pafs.S3FileSystem:
    endpoint = os.getenv("S3_ENDPOINT_URL")  # only set for local S3-compatible stores
    if endpoint:
        scheme, _, host = endpoint.partition("://")
        return pafs.S3FileSystem(endpoint_override=host, scheme=scheme, allow_bucket_creation=False)
    return pafs.S3FileSystem(region=os.getenv("AWS_REGION"))


def resolve(uri: str) -> tuple[pafs.FileSystem, str]:
    """Map a URI to (filesystem, path-within-filesystem)."""
    if uri.startswith("s3://"):
        return _s3(), uri.removeprefix("s3://")
    path = uri.removeprefix("file://")
    return pafs.LocalFileSystem(), os.path.abspath(path)


def write_text(uri: str, text: str) -> None:
    fs, path = resolve(uri)
    parent = path.rsplit("/", 1)[0]
    fs.create_dir(parent, recursive=True)
    with fs.open_output_stream(path) as f:
        f.write(text.encode())


def read_text(uri: str) -> str:
    fs, path = resolve(uri)
    with fs.open_input_stream(path) as f:
        return f.read().decode()


def exists(uri: str) -> bool:
    fs, path = resolve(uri)
    return fs.get_file_info(path).type != pafs.FileType.NotFound
