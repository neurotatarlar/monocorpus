from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Boolean, JSON

Base = declarative_base()

class Document(Base):
    """
    Represents a document record with metadata and storage information.

    Attributes:
        md5 (str): Unique MD5 hash of the document, used as the primary key.
        mime_type (str): MIME type of the document (e.g., 'application/pdf').
        ya_path (str): Path to the document file in the storage system. Can be obsolete.
        ya_public_url (str): Public URL to the document on Yandex Disk.
        ya_public_key (str): Public key for accessing the document on Yandex Disk.
        ya_resource_id (str): Resource identifier on Yandex Disk.
        publisher (str): Name of the document's publisher.
        author (str): Name(s) of the document's author(s).
        title (str): Title of the document.
        isbn (str): International Standard Book Number.
        publish_date (str): Date(mostly just year) when the document was published.
        language (str): Language in which the document in format BCP-47
        genre (str): Genre or category of the document.
        translated (bool): Indicates if the document is a translation.
        page_count (int): Number of pages in the document.
        content_extraction_method (str): Method used for content extraction.
        metadata_extraction_method (str): Method used for metadata extraction.
        full (bool): Indicates if the document is available in complete variant, not just a slice
        restrict_sharing(bool): Indicates if the document is not allowed for sharing and therefore links to it is encrypted
        document_url (str): URL to access the document.
        content_url (str): URL to access the document's content.
        metadata_json: (str): Metadata in JSON-LD format compatible with schema.org. 
        upstream_metadata_url (str): URL to upstream or original metadata source.
    """
    __tablename__ = "document"

    md5 = Column(primary_key=True, nullable=False, unique=True, index=True)
    mime_type = Column(String)
    ya_path = Column(String)
    ya_public_url = Column(String)
    ya_public_key = Column(String)
    ya_resource_id = Column(String)
    publisher = Column(String)
    author = Column(String)
    title = Column(String)
    isbn = Column(String)
    publish_date = Column(String)
    language = Column(String)
    genre = Column(String)
    translated = Column(Boolean)
    page_count = Column(Integer)
    content_extraction_method = Column(String)
    metadata_extraction_method = Column(String)
    full = Column(Boolean)
    sharing_restricted=Column(Boolean)
    document_url = Column(String)
    content_url = Column(String)
    metadata_json = Column(JSON)
    upstream_metadata_url=Column(String)

    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
        
    def __repr__(self):
        return self.__str__()