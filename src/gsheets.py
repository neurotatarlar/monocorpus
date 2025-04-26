from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

import typer
from monocorpus_models import Document, Session
from rich.progress import track
from sqlalchemy import select

def find_all_without_metadata():
    """
    Returns all documents without metadata.
    :return: List of documents without metadata.
    """
    stmt = select(Document).where(Document.metadata_url.is_(None))
    return Session().select(stmt)

def remove_file(md5):
    """
    Removes a document from the database by md5.
    :param md5: The md5 hash of the document to remove.
    :return: None
    """
    with Session()._create_session() as s:
        stmt = select(Document).where(Document.md5.is_(md5))
        res = s.scalars(stmt).all()
        if res:
            for doc in res:
                s.delete(doc)
            s.commit()

def find_by_file_name(file_name):
    """
    Removes a document from the database by file name.
    :param file_name: The file name to remove.
    :return: None
    """
    stmt = select(Document).where(Document.file_name.is_(file_name))
    return Session().select(stmt)

def get_all_md5s():
    """
    Returns a dict of all md5s in the database with ya_resource_id
    :return: set of md5s
    """
    res = Session()._create_session().execute(
        select(Document.md5, Document.ya_resource_id)
    ).all()
    return { i[0]: i[1] for i in res if i[1] is not None }

def find_by_md5(md5):
    stmt = select(Document).where(Document.md5.is_(md5)).limit(1)
    res = Session().select(stmt)
    return res[0] if res else None


def find_by_md5_non_complete(md5s, batch_size=500):
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    results = []
    chunks = list(chunks(md5s, batch_size))
    s = Session()
    for ch in chunks:
        stmt = select(Document).where(
            Document.md5.in_(ch)
            &
            Document.annotation_completed.isnot(True)
        )
        results.extend(s.select(stmt))
    return results


def find_all_annotations_completed_and_not_extracted():
    stmt = select(Document).where(
        Document.annotation_completed.is_(True)
        &
        Document.text_extracted.isnot(True)
    )
    return Session().select(stmt)


def upsert(doc):
    Session().upsert(doc)


def upsert_many_in_parallel(docs):
    with ThreadPoolExecutor(max_workers=1) as executor:
        f = [executor.submit(upsert, doc) for doc in docs]

        for future in track(futures.as_completed(f), description="Uploading changed documents", total=len(docs)):
            if exception := future.exception():
                executor.shutdown(wait=True, cancel_futures=True)
                print(f"Failed to upload document: {exception}")
                raise typer.Abort()

            yield future.result()