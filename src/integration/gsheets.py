import typer
from monocorpus_models import Document, Session
from rich.progress import track
from sqlalchemy import select
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures

def find_by_md5(md5):
    with Session() as s:
        stmt = select(Document).where(Document.md5.is_(md5)).limit(1)
        res = s.select(stmt)
        return res[0] if res else None

def find_by_md5_non_complete(md5s, batch_size=500):
    def chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

    results = []
    with Session() as s:
        chunks = list(chunks(md5s, batch_size))
        for ch in chunks:
            stmt = select(Document).where(
                Document.md5.in_(ch)
                &
                Document.annotation_completed.isnot(True)
            )
            results.extend(s.select(stmt))
    return results


def upsert(doc):
    with Session() as s:
        s.upsert(doc)

def upsert_many_in_parallel(docs):
    with ThreadPoolExecutor(max_workers=1) as executor:
        f = [executor.submit(upsert, doc) for doc in docs]

        for future in track(futures.as_completed(f), description="Uploading changed documents", total=len(docs)):
            if exception := future.exception():
                executor.shutdown(wait=True, cancel_futures=True)
                print(f"Failed to upload document: {exception}")
                raise typer.Abort()

            yield future.result()

