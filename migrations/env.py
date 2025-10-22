from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from src.models import Base
import yaml

config = context.config
fileConfig(config.config_file_name)
target_metadata = Base.metadata


def read_config(config_file: str = "config.yaml"):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def run_migrations_offline():
    database_url = read_config()['database_url']
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()

def run_migrations_online():
    database_url = read_config()['database_url']
    connectable = engine_from_config(
        {"sqlalchemy.url": database_url},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()

if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()

