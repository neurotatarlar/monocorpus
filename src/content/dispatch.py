from monocorpus_models import Document, Session, SCOPES
from yadisk_client import YaDisk
import mdformat
from dirs import Dirs
from rich import print
from s3 import upload_file, create_session
import os
import zipfile
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from queue import Queue
from utils import read_config, obtain_documents, download_file_locally, get_in_workdir, encrypt
from .epub_extractor import EpubExtractor
from .doc_like_extractor import DocLikeExtractor, to_docx_mime_types, check_encoding_mime_types
import threading
import time
from .pdf_extractor import PdfExtractor


non_pdf_format_types = to_docx_mime_types | \
    check_encoding_mime_types | \
    set(
        [
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document', 
            "text/markdown",
            'application/epub+zip',
        ]
    )

too_expensive = {
    "933a9910f0f7e1a5efd21aa1cd39e36e",
    "7ddc45e6fa6ed4caa120b11689cf200e",
    "7e18fc2e65badafaeacd3503fcb8df46",
    "2d7b5f5732a0144fe0fcf0c44cffc926",
    "ced45598a9cc9b331e1529c89ad0c77a",
    "e9eb18e8ba5f694f3de49a63f43a6255",
    "2f974ec14f30e05954f2748899a078b2",
    "f3e9b4311f6506f1ceb0f6f4b4de5f54",
    "395e748bdd6d6bf129925a3b616610f8",
    "6efdfcdbee76e39b1f947642e0ae0a11",
    "ad03cef6565ad757d6c2d47f159d5a5d",
    "15b4893c6cd99195774548ca4276d06d",
    "d601f93e8ce2cb4e3bc7dd6feac91a00",
    "a2b2ff6423020c31dfc3e85940f24255",  
    "914632a4bbf6c6c663a77b0e9e9d7bfa",
    "29b3429e9f1e1e31c2a89ebc24f9a073",
    "2d153f45e769759a8a75742c34bda846",
    "de06112b86863a0696ce7486d920efe4",
    "c3f1358a7d04d8efc051d94c5943c946",
    "81b2b8e133d6f61adf6dd3023da686ae",
    "5d39267ba7f82ad8ba813067dd2e14d6",  
    "acaf023d731e6f46627f34b291103f8d",
    "35e278f0b4cdb38c350dad01ccc915c3", 
    "c47a47587ad50b26317232486a9150a6",
    "d8e154dee7ae0ca66c44dfca5e0c6b6e", 
    "66fb1f7c96aea07e4431ff5fe55ab476",
    "3388c2aa2ca2d219af9211fce849d815",
    "bbd9f7f9571224e8e2e2abd6e9beb7d5",
    "490cd5879ea5a7dacbbcd41630633ec7",    
    "9099fc243084336a3d4a5bcd1b06b571",
    "12e94fe079fdf11f9414aa3c59a807dc",
    "aaa2556dd51b33e95f476837b6effe79",
    "ccda7afd6ad404ff7d352fa9d204d58c", 
    'ec9ef0cdc988d2cb8b49f95ea0d1201b',
    '1fa8ab2b88f7249375fcb612f5046e05',
    '962a1464b3399e9e6f5b5cd69693e670',
    '54b44249d1ea90a279e4ab0cdd9752c6',
    'fa2fbb8c5f8e1f1650df7606b07aba2d',
    '7bee22a24d3d08eb985255abcad73f9a',
    'f7445cd9403c1d44e042cc4e815941a2',
    '602d02ced4d0b2aa8bcd34e26dcbfc58',
    '993abe6cfd09b40afdcf4b39eddec115',
    '6767031d9b0a44dbc051a7857011490f',
    "c74e15c342cd45bd877e0b6fb0bc2af8",
    '107984a814779344aefd65410b9c1e84',
    'a76477128a14422d45383dda39477912',
    '91a0c3ac41c48c4cf6776a67e62d1f24',
    '893bcc71b541cebb269c2f154bd95baf',
    '1120f5cb71de4cf7b6f8b80b2f9ca8c7',
    'e56b6a4119b75b33c5320e60c1867249',
    'aebaa8474695ea06429537584419c1ec',
    '56443fb769237020851bc5ccfa234cca',
    '95d0e85bcbe2f0da25e13dd49729fd31',
    '22a8af0eabc269d41e6d22e3646da9e8',
    '4bde1a9aabb6f6c7f5ead82aa51f8d27'
}


# these docs skipped because they are processed by external contributor
skipped_external = {
    "7ddc45e6fa6ed4caa120b11689cf200e",
    "23e247a5cf94523a26cef1baeca08330",
    "31ce8173d68e9d6ad43beb520d9e9448",
    "07cc4822f3e37effa20c74d10eff387a",
    "a9b1da6ea3a12aedb6ed27093eca1bce",
    "883d9c8190d42250a5081e1b7e5635d9",
    "ecdfa76d4ca720f647c4e03969cb052b",
    "e1e129d1fecae4ae7e97a487823d6e3b",
    "f026fd136cfdc31146eae9627f897d0a",
    "82c2ae6276da4ed305657700e0a3eb95",
    "c8698eaa01752a239d5779553ba8797e",
    "6350d3bebd8612c5d1f85d470c16f8f9",
    "b305bbcd3644e9a0cc5e74116d444727",
    "3996366e00a2398971f27b3d866b1f8d",
    "94c14cf503df51ebc166100d3b156116",
    "bb1499278121c478aff5a295f378b817",
    "cf97d9e734afe487b405b192a2b9132a",
    "a32a083aae191391afa1f0f0ad5612f6",
    "28797210aad79878bfca2f36c9ceffb2",
    "2284187654c2384c98bc2f218f4a4a31",
    "bc3baa864c4eb5b7ee16bcc693beeb3f",
    "fdb3ca5fec275257a473a078f5357762",
    "49d84164271052f59047aa55059ff354",
    "1c20250dcc2dfe2a576836209910eda2",
    "febbc761113bbb62d53d9d44b8aae03f",
    "85909aeddf6aad90af5e133647916a5c",
    "bb8054f3f97e6d8c24952747896ce798",
    "cb3a30518de60f86ac5a9320ddbc359f",
    "566ad47e7fa9d62de1aa6e718a51eefc",
    "9026cbbd642fe7e11263d1d93f341e46",
    "6e4d024cc644868d8cd4b1a61e6e6e01",
    "567e836d1d0b3e4844136298ff478e4d",
    "0ac2b8526619d90a033f555e96824241",
    "1aa8a0f53a6eb7d1a80fd6f277b1461b",
    "bb36a0f7472ad8bbf042e1808059e986",
    "a2836964850cfc7a0aa60c9d84238b67",
    "d17e958165101c38cbe54802cbf3ccfb",
    "33d340001666758a941f45d8e52918d1",
    "2b74413020def6d1721d2b4cebacadc4",
    "5bc67f299737246e0f158eda5f25613b",
    "83fd6bbe968f6d5927ff461d09ea4bad",
    "16f2434b740ee116ac6f634f35977345",
    "a2aee670bcf2824596cf1a2e82f7af11",
    "581d6547cdf1f7929541907285ddb56d",
    "d1ac7329ea0ada8a4f9382a63a59ddec",
    "1457cc34b6d426459b6bfcba4136a9f7",
    "4426f343d867a49c5c8b91a1af48e7f7",
    "57878b070e6074f42fe72bc09369b024",
    "fec950bab89ad759a306b26e38b71259",
    "359db1b930db12ddbd2697a119c7872e",
}

skip_pdf = skipped_external | too_expensive


def extract_content(cli_params):
    print("Extracting content of nonpdf documents")
    predicate = (
        Document.content_url.is_(None) &
        Document.mime_type.in_(non_pdf_format_types)
    )
    _process_non_pdf_by_predicate(predicate, cli_params)
    
    # print("Extracting content of pdf documents")
    # predicate = (
    #     Document.content_url.is_(None) &
    #     Document.mime_type.is_("application/pdf")
    # )
    # _process_pdf_by_predicate(predicate, cli_params)
    
 
def _process_non_pdf_by_predicate(predicate, cli_params):
    config = read_config()
    s3client = create_session(config)
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client, Session() as gsheets_session:
        docs = obtain_documents(cli_params, ya_client, predicate)
        if not docs:
            print("No documents for processing...")
            return
        
        gcloud_creds = _get_credentials()
        for doc in docs:
            print(f"Extracting content from file {doc.md5}({doc.ya_public_url})")
            local_doc_path = download_file_locally(ya_client, doc, config)
            if doc.mime_type == 'application/epub+zip':
                content = EpubExtractor(doc, local_doc_path, config, s3client).extract()
            else:
                content = DocLikeExtractor(doc, local_doc_path, config, s3client, gcloud_creds).extract()
            
            formatted_content = mdformat.text(
                content,
                codeformatters=(),
                extensions=["toc", "footnote"],
                options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
            )
            formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}-formatted.md")
            with open(formatted_response_md, 'w') as f:
                f.write(formatted_content)
                
            _upload_artifacts_to_s3(doc, formatted_response_md, local_doc_path, config, s3client)

            gsheets_session.update(doc)
            
            
def _upload_artifacts_to_s3(doc, formatted_response_md, local_doc_path, config, s3lient):        
    content_key = f"{doc.md5}.zip"
    content_bucket = config["yandex"]["cloud"]['bucket']['content']
    local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{doc.md5}.zip")
    with zipfile.ZipFile(local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{doc.md5}.md", filename=formatted_response_md)
    doc.content_url = upload_file(local_content_path, content_bucket, content_key, s3lient)
    
    doc_bucket = config["yandex"]["cloud"]['bucket']['document']
    doc_key = os.path.basename(local_doc_path)
    remote_doc_url = upload_file(local_doc_path, doc_bucket, doc_key, s3lient, skip_if_exists=True)
    doc.document_url = encrypt(remote_doc_url, config) if doc.sharing_restricted else remote_doc_url


def _get_credentials():
    token_file = "personal_token.json"
    
    if os.path.exists(token_file):
        return Credentials.from_authorized_user_file(token_file, SCOPES)
    
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
    with open(token_file, 'w') as f:
        f.write(creds.to_json())
    return Credentials.from_authorized_user_file(token_file, SCOPES)

    
def _process_pdf_by_predicate(predicate, cli_params, docs_batch_size=72, keys_batch_size=1, offset=150):
    config = read_config()
    exceeded_keys_lock = threading.Lock()
    exceeded_keys_set = set()
    stop_event = threading.Event()
    while not stop_event.is_set():
        tasks_queue = None
        threads = None
        try:
            with exceeded_keys_lock:
                available_keys =  set(config["gemini_api_keys"]) - exceeded_keys_set
            keys_slice = list(available_keys)[:keys_batch_size]
            if not keys_slice:
                print("No keys available, exiting...")
                return
            else:
                print(f"Available keys: {available_keys}, Total keys: {config['gemini_api_keys']}, Exceeded keys: {exceeded_keys_set}, Extracting with keys: {keys_slice}")
            
            with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
                with Session() as gsheets_session:
                    docs = list(obtain_documents(cli_params, ya_client, predicate, limit=docs_batch_size, gsheet_session=gsheets_session, offset=offset))

                print(f"Got {len(docs)} docs for content extraction")
                tasks_queue = Queue(maxsize=len(docs))
                for doc in docs:
                    if doc.md5 not in skip_pdf:
                        tasks_queue.put(doc)
                    
                if tasks_queue.empty():
                    print("No documents for processing...")
                    return
                
                s3lient = create_session(config)
                    
                threads = []
                for num in range(min(len(keys_slice), len(docs))):
                    key = keys_slice[num]
                    t = threading.Thread(target=PdfExtractor(key, tasks_queue, config, s3lient, ya_client, exceeded_keys_lock, exceeded_keys_set, stop_event))
                    t.start()
                    threads.append(t)
                    time.sleep(5)  # slight delay to avoid overwhelming the API with requests

            # waiting for workers shutdown gracefully
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            print("Interrupted, shutting down workers...")
            stop_event.set()
            if tasks_queue:
                tasks_queue.queue.clear()  # Clear the queue to stop workers
            if threads:
                for t in threads:
                    t.join(timeout=60*10)
            return