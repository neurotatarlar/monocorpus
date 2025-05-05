from monocorpus_models import Document, Session
from sqlalchemy import select

def find_all(predicate=None, md5s = None, limit=None):
    # here md5s is a separate parameter because having it as predicate 
    # is extremely long - faster to exclude on client's side 
    stmt = _prepare(predicate, limit=None)
    docs = Session().select(stmt)
    if docs and md5s:
        docs = [d for d in docs if d.md5 in md5s] 
    if docs and limit:
        docs = docs[:limit]
    return docs or []

def find_one(predicate):
    stmt = _prepare(predicate, limit=1)
    res = Session().select(stmt)
    return res[0] if res else None
    
def _prepare(predicate, limit):
    stmt = select(Document)
    if predicate is not None:
        stmt = stmt.where(predicate)
    if limit is not None:
        stmt = stmt.limit(limit)
    return stmt

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

def get_all_md5s():
    """
    Returns a dict of all md5s in the database with ya_resource_id
    :return: set of md5s
    """
    res = Session()._create_session().execute(
        select(Document.md5, Document.ya_resource_id, Document.upstream_metadata_url)
    ).all()
    return { 
            i[0]: {"resource_id": i[1], "upstream_metadata_url": i[2]} 
            for i 
            in res 
            if i[1] is not None
    }

def upsert(doc):
    Session().upsert(doc)