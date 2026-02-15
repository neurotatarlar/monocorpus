"""Tests for sync storage cleanup helpers."""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import Mock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from sync.storage import _delete_keys, remove_from_s3  # noqa: E402


class _FakePaginator:
    def __init__(self, pages_by_bucket_prefix):
        self.pages_by_bucket_prefix = pages_by_bucket_prefix
        self.calls = []

    def paginate(self, **kwargs):
        bucket = kwargs["Bucket"]
        prefix = kwargs.get("Prefix")
        self.calls.append((bucket, prefix))
        return self.pages_by_bucket_prefix.get((bucket, prefix), [])


class SyncStorageTests(unittest.TestCase):
    def test_remove_from_s3_uses_targeted_prefix_listing(self) -> None:
        s3client = Mock()
        cfg = {
            "yandex": {
                "cloud": {
                    "bucket": {
                        "content": "content",
                        "content_chunks": "chunks",
                        "document": "docs",
                        "image": "images",
                        "upstream_metadata": "upstream",
                        "metadata": "metadata",
                    }
                }
            }
        }
        paginator = _FakePaginator(
            {
                ("chunks", "a/"): [{"Contents": [{"Key": "a/chunk-1.zip"}]}],
                ("docs", "a"): [{"Contents": [{"Key": "a.pdf"}]}],
                ("images", "a"): [{"Contents": [{"Key": "a-0-0.png"}]}],
                ("chunks", "b/"): [],
                ("docs", "b"): [],
                ("images", "b"): [],
            }
        )
        s3client.get_paginator.return_value = paginator

        remove_from_s3(["a", "b"], s3client, cfg)

        # Ensure there was no full-bucket pagination call without Prefix.
        self.assertTrue(all(prefix is not None for _, prefix in paginator.calls))
        self.assertIn(("chunks", "a/"), paginator.calls)
        self.assertIn(("docs", "a"), paginator.calls)
        self.assertIn(("images", "a"), paginator.calls)

        delete_calls = s3client.delete_objects.call_args_list
        deleted_by_bucket = {}
        for call in delete_calls:
            kwargs = call.kwargs
            bucket = kwargs["Bucket"]
            keys = [item["Key"] for item in kwargs["Delete"]["Objects"]]
            deleted_by_bucket.setdefault(bucket, []).extend(keys)

        self.assertEqual(["a.zip", "b.zip"], deleted_by_bucket["content"])
        self.assertEqual(["a-meta.zip", "b-meta.zip"], deleted_by_bucket["metadata"])
        self.assertEqual(["a.zip", "b.zip"], deleted_by_bucket["upstream"])
        self.assertEqual(["a/chunk-1.zip"], deleted_by_bucket["chunks"])
        self.assertEqual(["a.pdf"], deleted_by_bucket["docs"])
        self.assertEqual(["a-0-0.png"], deleted_by_bucket["images"])

    def test_delete_keys_batches_in_thousands(self) -> None:
        s3client = Mock()
        keys = [f"k{i}" for i in range(2001)]
        _delete_keys(s3client, "bucket", keys)

        self.assertEqual(3, s3client.delete_objects.call_count)
        first = s3client.delete_objects.call_args_list[0].kwargs["Delete"]["Objects"]
        second = s3client.delete_objects.call_args_list[1].kwargs["Delete"]["Objects"]
        third = s3client.delete_objects.call_args_list[2].kwargs["Delete"]["Objects"]
        self.assertEqual(1000, len(first))
        self.assertEqual(1000, len(second))
        self.assertEqual(1, len(third))

