import json
import os.path
from datetime import datetime

import portion as P
from rich import print
from rich.progress import track

from consts import Dirs
from dispatch import layout_analysis_entry_point
from file_utils import read_config, get_path_in_workdir
from integration.gsheets import find_by_md5_non_complete
from integration.gsheets import upsert_many_in_parallel
from integration.s3 import list_files, create_session, download_in_parallel, download_annotation, remove_objects
from integration.s3 import upload_files_to_s3
from intersections import compute_intersection_area


def sync():
    # download all annotations from the bucket
    downloaded_annotations = _download_annotations()
    # transform annotations to dictionary and reduce annotations of the same page by keeping only the last one per page
    transformed = _transform_annotations(downloaded_annotations)
    _calculate_completeness(transformed)
    print("Sync completed successfully")


def _download_annotations():
    config = read_config()
    session = create_session(config)
    bucket = config['yc']['bucket']['annotations']
    print("Getting the list of annotations from the S3 bucket...")
    remote_annot = list_files(bucket, session=session)
    print(f"Found {len(remote_annot)} annotations in the S3 bucket")
    downloaded_annotations = []
    download_folder = get_path_in_workdir(Dirs.ANNOTATIONS)

    for res in download_in_parallel(remote_annot, download_annotation, bucket, download_folder, session):
        downloaded_annotations.append(res)

    return downloaded_annotations


def _transform_annotations(downloaded_annotations):
    transformed_annotations = {}
    for path_to_f in track(downloaded_annotations[:], description="Processing downloaded annotations..."):
        with open(path_to_f, 'r') as file:
            anno = json.load(file)
            data = anno['task']['data']
            if not (doc_md5 := data.get('hash')):
                print(f"Document `{path_to_f}` has no md5 hash, skipping it...")
                continue
            res = {
                'anno_id': anno['id'],
                'image_link': data['image'],
                'last_changed': datetime.strptime(anno['task']['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%s'),
                'results': [
                    {
                        "original_width": r['original_width'],
                        "original_height": r['original_height'],
                        "x": r['value']['x'],
                        "y": r['value']['y'],
                        "width": r['value']['width'],
                        "height": r['value']['height'],
                        'class': r['value']['rectanglelabels'][0],
                    }
                    for r
                    in anno['result']
                    if r.get('value') and r['value']['rectanglelabels'] not in ['caption']
                ]
            }
            _append_annotation(transformed_annotations, doc_md5, data['page_no'], res)

    valid_annotations, intersected = find_intersections(transformed_annotations)
    if intersected:
        print(f"Found {len(intersected)} docs with intersections")
        for md5, prelabel in intersected.items():
            layout_analysis_entry_point(md5=md5, force=True, prelabel=prelabel)

    valid_annotations, anomalies = find_semantic_anomalies(valid_annotations)
    if anomalies:
        print(f"Found {len(anomalies)} docs with semantic anomalies")
        for md5, prelabel in anomalies.items():
            layout_analysis_entry_point(md5=md5, force=True, prelabel={k: {} for k, v in prelabel.items()})

    return valid_annotations


def _append_annotation(accumulator, doc_hash, page_no, res):
    if doc_hash not in accumulator:
        accumulator[doc_hash] = {}
    if page_no not in accumulator[doc_hash]:
        accumulator[doc_hash][page_no] = res
    elif accumulator[doc_hash][page_no]['last_changed'] < res['last_changed']:
        accumulator[doc_hash][page_no] = res


def _calculate_completeness(transformed):
    all_md5s = list(transformed.keys())
    print(f"Requesting documents from the remote datastore...")
    docs = {doc.md5: doc for doc in find_by_md5_non_complete(all_md5s)}
    if not docs:
        print("Incomplete documents not found, returning...")
        return
    else:
        print(f"Found {len(docs)} not completed documents in the remote datastore")

    # list of docs that were changed and need to be updated in remote datastore
    changed_docs = []
    # list of completed annotations to be saved in the object storage
    cas = {}
    for doc_hash, pages in track(transformed.items(), description="Calculating completeness..."):
        if not (doc := docs.get(doc_hash)):
            # here we can skip the document if it is not found in the remote datastore
            # it means that the document was already marked as completed
            continue

        completeness = round(len(pages) / doc.pages_count, 2)
        completed = completeness == 1.0

        if not completed:
            missing_pages = _missing_pages(pages.keys(), doc.pages_count)
            url = doc.ya_public_url or "url not found"
            print(f"Document {doc_hash}({url}) is not completed: {completeness}, missing pages: {missing_pages}")

        if completeness == doc.completeness and not completed:
            # here we can skip the document if the completeness is the same and the document is not completed
            # this is done to avoid unnecessary updates in the remote datastore
            continue

        if completed:
            doc.annotation_completed = True
            cas[doc_hash] = pages

        doc.sent_for_annotation = True
        doc.completeness = completeness
        changed_docs.append(doc)

    # upload completed annotations to the object storage
    _upload_annotation_summaries(cas)

    # update the remote datastore
    for _ in upsert_many_in_parallel(changed_docs):
        pass


def _missing_pages(pages, pages_count):
    i = P.empty()
    for page_no in pages:
        i |= P.closed(page_no, page_no + 1)

    return P.open(0, pages_count) - i


def _upload_annotation_summaries(cas):
    paths = []
    for doc_hash, pages in cas.items():
        output_file = os.path.join(get_path_in_workdir(Dirs.ANNOTATION_RESULTS), f"{doc_hash}.json")
        with open(output_file, 'w') as f:
            json.dump(pages, f, indent=4, ensure_ascii=False, sort_keys=True)
        paths.append(output_file)

    upload_files_to_s3(paths, lambda c: c['yc']['bucket']['annotations_summary'], )


def find_intersections(transformed_annotations):
    def _page_has_intersections(_coords):
        _n = len(_coords)
        for i in range(_n):
            for j in range(i + 1, _n):  # Avoid duplicate pairs and self-comparison
                if (ia := compute_intersection_area(coords[i], coords[j])) > 0:
                    return ia, coords[i], coords[j]
        return None

    valid_annotations = {}
    invalid_annotations = {}
    for md5, doc in transformed_annotations.items():
        prelabel = {}
        for page_no, page in doc.items():
            coords = []
            results = page['results']
            for annotation in results:
                x_min = annotation['x']
                y_min = annotation['y']
                x_max = x_min + annotation['width']
                y_max = y_min + annotation['height']
                coords.append([x_min, y_min, x_max, y_max])

            if _page_has_intersections(coords):
                prelabel[page_no] = page
            else:
                valid_annotations[md5] = doc

        if prelabel:
            invalid_annotations[md5] = prelabel

    return valid_annotations, invalid_annotations

def find_semantic_anomalies(annotations):
    valid_annotations = {}
    anomalies = {}
    for md5, doc in annotations.items():
        prelabel = {}
        for page_no, page in doc.items():
            prev_class_ = None
            sorted_layout = sorted(page["results"], key=lambda x: (x["y"], x["x"]))
            for r in sorted_layout:
                class_ = r['class']
                if class_ == 'title' and page_no > 4:
                    print(f"Found incorrect title on a page {page_no}")
                    prelabel[page_no] = page
                    break
                elif class_ == 'page-header' and prev_class_ and prev_class_ != 'page-header':
                    print(f"Found incorrect header on a page {page_no}")
                    prelabel[page_no] = page
                    break
                else:
                    prev_class_ = class_
            prev_class_ = None
            for r in reversed(sorted_layout):
                class_ = r['class']
                if class_ == 'page-footer' and prev_class_ and prev_class_ != 'page-footer':
                    print(f"Found incorrect footer on a page {page_no}")
                    prelabel[page_no] = page
                    break
                else:
                    prev_class_ = class_


        if prelabel:
            anomalies[md5] = prelabel

    return valid_annotations, anomalies

