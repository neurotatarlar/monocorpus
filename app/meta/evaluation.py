from pydantic import BaseModel
from db.models import Document
from utils import get_session, read_config, dump_expired_keys
from sqlalchemy import select
from rich import print
from queue import Queue, Empty
import re 
from yadisk_client import YaDisk
import threading
import time
import random
from gemini import create_client
from google.genai.errors import ClientError
import datetime
import os


# update json-ld schema with DDC taxonomy
# print tokens count usage for cost estimation

legal_docs_pattern = [
    re.compile(r'^(?=.*common_crawl)(?=.*npa_ta_).*\.pdf$'),
    re.compile(r'^(?=.*pdf законов с pravo\.gov).*\.pdff$')
]


class Evaluation(BaseModel):
    applicable: bool = True
    reason: str = None

    def nonapplicable(reason) -> "Evaluation":
        _eval = Evaluation()
        _eval.applicable = False
        _eval.reason = reason
        return _eval

def evaluate(args):
    """
    Decide if books is applicable for library management and create taxonomy
    """
    # 1. Get documents for processing
    # 2. Filter eraly non applicable for library management
    # 3. For each document create prompt and request gemini to get taxonomy
    config = read_config()
    for lang_code in config['sup_langs']:
        _process_lang(args, lang_code, config)
        
            
def _process_lang(args, lang_code, config):
    params = config['sup_langs'][lang_code]
    predicate = _get_predicate(params['codes'])
    stop_event = threading.Event()
    channel = Channel()
    
    while not stop_event.is_set():
        if not channel.available_keys:
            print("No gemini keys available, exiting...")
            return      
        workers = []
        with get_session() as session:
            # get batch of documents for processing
            docs = session.scalars(select(Document).where(predicate).limit(args.batch_size)).all()
            
        # filter early non applicable documents  
        docs, non_applicables = _early_skip(docs)
        if non_applicables:
            _save_non_applicable(non_applicables)
        
        tasks_queue = _create_queue(docs)
        if tasks_queue.empty():
            print("No more documents to process")
            return
        else:
            print(f"Processing batch of {tasks_queue.qsize()} documents")
            
        try:
            with YaDisk(config['yandex']['disk']['oauth_token'], proxy=config['proxy']) as ya_client:
                for _ in range(min(len(channel.available_keys), len(docs), args.workers)):
                    worker = LibraryApplicabilityWorker(tasks_queue=tasks_queue)
                    t = threading.Thread(target=worker)
                    t.start()
                    workers.append(t)
                    time.sleep(5)  # slight delay to avoid overwhelming the API with requests

            # waiting for workers shutdown gracefully
            for t in workers:
                t.join()
        except KeyboardInterrupt:
            print("Interrupted, shutting down workers...")
            stop_event.set()

            for t in (workers or []):
                t.join(timeout=120)
            return
        finally:
            dump_expired_keys()
        
def _get_predicate(codes):
    return (
        Document.lib.is_(None)
        &
        Document.language.in_(codes)
        &
        Document.meta.is_not(None)
    )
    

def _early_skip(docs):
    probables = []
    non_applicables = []
    for doc in docs:
        if doc.full is not True:
            non_applicables.append((doc, "not full"))
            continue
        elif doc.sharing_restricted is True:
            non_applicables.append((doc, "sharing restricted"))
            continue
        elif doc.isbn:
            probables.append(doc)
            continue
        elif any(pattern.match(doc.ya_path) for pattern in legal_docs_pattern):
            print(f"Skipping legal doc {doc.ya_path}")
            non_applicables.append((doc, "legal doc"))
            continue
        else:
            probables.append(doc)
            continue
    return probables, non_applicables
            
            
def _save_non_applicable(non_applicables):
    print(f"Marking {len(non_applicables)} documents as non applicable for library management")
    with get_session() as session:
        for doc, reason in non_applicables:
            _eval = Evaluation.nonapplicable(reason)
            doc.lib = _eval.model_dump_json(indent=4, ensure_ascii=False)
            session.add(doc)
        session.commit()
            

def _create_queue(docs):
    tasks_queue = Queue()
    for doc in docs:
        tasks_queue.put(doc)
    return tasks_queue


def _get_keys(config, channel):
    available_keys = list(set(config['gemini']['api_keys'] - channel.exceeded_keys_set))
    random.shuffle(available_keys)
    return available_keys


class LibraryApplicabilityWorker:
    def __init__(self, gemini_api_key, tasks_queue, config):
        self.key = gemini_api_key
        self.tasks_queue = tasks_queue
        self.config = config

        
    def __call__(self):
        gemini_client = create_client(self.key)
        prev_req_time = None
        while True:
            try:
                doc = self.tasks_queue.get(block=False)
                self.log(f"Evaluating doc {doc.md5} ({doc.ya_path})")
                prev_req_time = self._sleep_if_needed(prev_req_time)
                evaluation = self._evaluate(doc, gemini_client)
                if not evaluation:
                    self.log(f"Could not evaluate document {doc.md5}({doc.ya_public_url})")
                    self._dump_unprocessables(doc.md5)
                    continue

            except Empty:
                self.log("No tasks for processing, shutting down thread...")
                return
            except ClientError as e:
                print(f"ClientError during metadata extraction for doc '{doc.md5}({doc.ya_path})' with key '{self.key}': {e}")
                self._dump_unprocessables(doc.md5)
                if e.code == 429:
                    self.log(f"Key {self.key} exhausted {e}, shutting down thread...") 
                    self.tasks_queue.put(doc)
                    with self.exceeded_keys_lock:
                        self.exceeded_keys_set.add(self.key)
                    return
                continue
            except Exception as e:
                import traceback
                self.log(f"Could not extract metadata from doc {doc.md5}: {e} \n{traceback.format_exc()}")
                self._dump_unprocessables(doc.md5)
                continue

    def _sleep_if_needed(self, prev_req_time):
        if prev_req_time:
            elapsed = datetime.datetime.now() - prev_req_time
            if elapsed < datetime.timedelta(minutes=1):
                time_to_sleep = int(65 - elapsed.total_seconds()) + 1
                self.log(f"Sleeping for {time_to_sleep} seconds")
                time.sleep(time_to_sleep)
        return datetime.datetime.now()
    
    
    def _evaluate(self, doc, gemini_client):
        pass

    
    
class Channel:
    
    def __init__(self):
        self.lock = threading.Lock()
        self.available_keys = set()
        self.exceeded_keys_set = load_expired_keys()
        self.unprocessable_docs, self.repairable_docs = self._load_unprocessable_docs()
        
    def get_all_unprocessable_docs(self):
        return self.unprocessable_docs | self.repairable_docs
    
    def dump(self):
        dump_expired_keys(self.exceeded_keys_set)
        self._dump_to_file("unprocessables", "unprocessables.txt", self.unprocessable_docs)
        self._dump_to_file("unprocessables", "repairables.txt", self.repairable_docs)
            

    def _load_unprocessable_docs(self, dir = "unprocessables"):
        return self._load_file(dir, "unprocessables.txt"), self._load_file(dir, "repairables.txt")
    
    
    def _load_file(self, dir, file_name):
        file = os.path.join(dir, file_name)
        if os.path.exists(file):
            with open(file, "r") as f:
                return set([l.strip() for l in f.readlines()])
        else: 
            return set()
        
        
    def _dump_to_file(self, dir, file_name, items):
        os.makedirs(dir, exist_ok=True)
        file = os.path.join(dir, file_name)
        with open(file, "w") as f:
            f.write("\n".join([l.strip() for l in items]))
            
    
    def add_exceeded_key(self, key):
        with self.lock:
            self.exceeded_keys_set.add(key)
            dump_expired_keys(self.exceeded_keys_set)
    
            
    def add_unprocessable_doc(self, md5):
        with self.lock:
            self.unprocessable_docs.add(md5)
            self._dump_to_file("unprocessables", "unprocessables.txt", self.unprocessable_docs)
            
    
    def add_repairable_doc(self, md5):
        with self.lock:
            self.repairable_docs.add(md5)
            self._dump_to_file("unprocessables", "repairables.txt", self.repairable_docs)