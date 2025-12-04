from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import yaml
from typing import Union
from dirs import Dirs
import os
import sys
import json


WORKDIR = "~/.monocorpus"


def get_engine(echo: bool = False):
    config = read_config()
    return create_engine(config['database_url'], echo=echo)


def get_session():
    Session = sessionmaker(bind=get_engine())
    return Session()


def read_config(config_file: str = "config.yaml"):
    with open(get_in_workdir(file=config_file, prefix="."), 'r') as file:
        return yaml.safe_load(file)
    
    
def get_in_workdir(*dir_names: Union[str, Dirs], file: str = None, prefix: str = WORKDIR):
    dir_names = [i.value if isinstance(i, Dirs) else i for i in dir_names]
    script_parent_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
    path = [script_parent_dir, '..', os.path.expanduser(prefix), *dir_names]
    path = os.path.normpath(os.path.join(*path))
    os.makedirs(path, exist_ok=True)
    if file:
        return os.path.join(path, file)
    else:
        return path

    
def dump_expired_keys(keys, dir = 'artifacts/expired_keys', file="expired_keys.json"):
    os.makedirs(dir, exist_ok=True)
    path = os.path.join(dir, file)
    with open(path, "r") as f:
        existing_keys = set(json.load(f))
    keys = existing_keys.union(set(keys))
    with open(path, "w") as f:
        json.dump(list(keys), f, ensure_ascii=False, indent=4)