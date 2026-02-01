"""Database session helpers (legacy)."""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from utils import read_config


def get_db():
    """Yield a SQLAlchemy session factory bound to the configured database."""
    db = None
    try:
        config = read_config()
        engine = create_engine(config['database_url'], echo=True)
        yield sessionmaker(autocommit=False, autoflush=False, bind=engine)
    finally:
        if db:
            db.close()
