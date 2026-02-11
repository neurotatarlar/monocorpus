"""Metadata ORM model."""

from sqlalchemy import JSON, Boolean, Column, ForeignKey, String
from sqlalchemy.orm import relationship

from .base import Base


class Metadata(Base):
    """One-to-one schema.org metadata and library applicability for a document."""

    __tablename__ = "metadata"

    md5 = Column(String, ForeignKey("document.md5", ondelete="CASCADE"), primary_key=True)
    schema_org = Column(JSON)
    lib = Column(Boolean)

    document = relationship("Document", back_populates="metadata_row")

    def __str__(self):
        return "%s(%s)" % (
            type(self).__name__,
            ", ".join("%s=%s" % item for item in vars(self).items()),
        )

    def __repr__(self):
        return self.__str__()


__all__ = ["Metadata"]
