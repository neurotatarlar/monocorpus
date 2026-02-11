"""Database and storage lookup helpers for sync."""

from __future__ import annotations

from sqlalchemy import select

from core.db import get_session
from models import Document, Metadata


def list_docs_with_schema_org(session):
    """Return docs that have schema.org metadata for ISBN dedup checks."""
    return list(
        session.scalars(
            select(Document)
            .join(Metadata, Metadata.md5 == Document.md5)
            .where(Metadata.schema_org.is_not(None))
        )
    )


def lookup_upstream_metadata(s3client, config):
    """Return a mapping of md5 to upstream metadata URL in S3."""
    bucket = config["yandex"]["cloud"]["bucket"]["upstream_metadata"]
    s3client.list_objects_v2(Bucket=bucket)
    paginator = s3client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    return {
        obj['Key'].removesuffix('.zip'): f"{s3client._endpoint.host}/{bucket}/{obj['Key']}"
        for page in pages
        for obj in page['Contents']
    }


def get_all_md5s(entity_cls, session_factory=get_session):
    """Return md5-indexed map of current document storage pointers."""
    with session_factory() as session:
        res = session.execute(
            select(entity_cls.md5, entity_cls.ya_resource_id, entity_cls.upstream_meta_url, entity_cls.ya_path, entity_cls.ya_public_url)
        ).all()
        return {
            i[0]: {
                "resource_id": i[1],
                "upstream_meta_url": i[2],
                "ya_path": i[3],
                "ya_public_url": i[4],
            }
            for i in res
        }
