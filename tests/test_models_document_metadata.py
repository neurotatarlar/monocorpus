"""Relationship behavior tests for Document/Metadata models."""

from __future__ import annotations

import os
import sys
import unittest

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.argv[0] = os.path.join(REPO_ROOT, "src", "main.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from models import Base, Document, Metadata  # noqa: E402


class DocumentMetadataRelationshipTests(unittest.TestCase):
    """Ensure deleting document does not attempt to null metadata PK."""

    def test_deleting_document_removes_metadata_row(self) -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        Base.metadata.create_all(engine)

        with Session(engine) as session:
            doc = Document(md5="a" * 32, mime_type="application/pdf")
            doc.metadata_row = Metadata(md5=doc.md5, schema_org={"name": "Book"})
            session.add(doc)
            session.commit()

            self.assertEqual(
                1,
                session.scalar(
                    select(func.count()).select_from(Document).where(Document.md5 == doc.md5)
                ),
            )
            self.assertEqual(
                1,
                session.scalar(
                    select(func.count()).select_from(Metadata).where(Metadata.md5 == doc.md5)
                ),
            )

            session.delete(doc)
            session.commit()

            doc_left = session.scalar(select(Document).where(Document.md5 == "a" * 32))
            meta_left = session.scalar(select(Metadata).where(Metadata.md5 == "a" * 32))
            self.assertIsNone(doc_left)
            self.assertIsNone(meta_left)


if __name__ == "__main__":
    unittest.main()
