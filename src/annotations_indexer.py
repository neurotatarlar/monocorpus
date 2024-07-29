import json
import os.path
from datetime import datetime

from rich.progress import track

from integration.airtable import Annotation, Document, AnnotationsSummary
from integration.s3 import list_files, create_session, download_annotations
from file_utils import read_config
from pyairtable.formulas import match
from integration.s3 import upload_files_to_s3
from file_utils import get_path_in_workdir
from consts import Dirs


def sync_annotations():
    config = read_config()
    session = create_session(config)
    bucket = config['yc']['bucket']['annotations']

    print("Getting the list of annotations from the Airtable...")
    checked_annot = {
        str(a['anno_id']): a['anno_md5']
        for a
        in
        map(lambda a: a.to_record().get('fields'), Annotation.all(fields=['anno_id', 'anno_md5']))
        if a
    }

    # Get the list of files in the S3 bucket
    remote_annot = list_files(bucket, session=session)

    # Find the files that are in the S3 bucket but not in the Airtable or has different md5
    diff = set(remote_annot.items()) - set(checked_annot.items())

    if not diff:
        print("No new annotations found in the S3 bucket")
        return

    print(f"Found {len(diff)} files in the S3 bucket that are not in the Airtable")
    # Download the annotations locally
    downloaded_files = download_annotations(bucket, diff, session=session)

    annotations_to_save = []
    docs_cache = {}
    for f, md5 in track(downloaded_files, description="Processing downloaded annotations..."):
        with open(f, 'r') as file:
            a = json.load(file)
            anno_id = a['id']
            data = a['task']['data']
            doc_md5 = data['hash']

            if md5 not in docs_cache:
                docs_cache[doc_md5] = Document.first(formula=f"md5='{doc_md5}'")

            # Check if the annotation with the same id is already in the Airtable, maybe the annotation was updated
            # in the object storage
            known_md5 = checked_annot.get(str(anno_id))
            if not known_md5:
                anno = Annotation(anno_id=anno_id)
            elif known_md5 != md5:
                print(f"MD5 mismatch for annotation `{anno_id}`. Known: {known_md5}, new: {md5}, updating...")
                anno = Annotation.first(formula=f"anno_id={anno_id}")
            else:
                # If we are here it means function to find the difference between the airtable and s3 is not working,
                # because here if id and md5 are same
                # Just skipping it...
                print(f"Annotation `{anno_id}` already in the Airtable")
                continue

            anno.page_no = data['page_no']
            anno.image_link = data['image']
            anno.anno_md5 = md5
            anno.doc_md5 = doc_md5
            anno.last_changed = datetime.strptime(a['task']['updated_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            anno.results = json.dumps([
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
                in a['result']
            ])
            annotations_to_save.append(anno)

    print(f"Saving {len(annotations_to_save)} annotations to the Airtable")
    Annotation.batch_save(annotations_to_save)
    for d in docs_cache.values():
        d.sent_for_annotation = True
    Document.batch_save([d for d in docs_cache.values()])
    print("Syncing is done!")


def calculate_completeness():
    """
    Calculate the completeness of the annotations for every document sent for annotation
    """

    # Get all documents that were sent for annotation but the annotation is not completed
    not_completed_docs = Document.all(
        fields=[Document.md5.field_name, Document.pages_count.field_name],
        formula=match({
            Document.sent_for_annotation.field_name: True,
            Document.annotation_completed.field_name: False
        })
    )

    # docs to update in the Airtable
    docs_to_update = []
    # annotation summaries to update in the Airtable
    anno_summaries_to_update = []

    has_tables = False
    has_images = False
    has_formulas = False

    for doc in not_completed_docs:
        # Get all annotations for the document
        related_annotations = Annotation.all(
            fields=[
                Annotation.doc_md5.field_name,
                Annotation.page_no.field_name,
                Annotation.last_changed.field_name,
                Annotation.results.field_name
            ],
            formula=f"{Annotation.doc_md5.field_name}='{doc.md5}'"
        )

        # key is page_no, value is the annotations for the page
        grouped = {}
        # Group the annotations by page_no and get the latest annotation for every page
        for a in related_annotations:
            if a.page_no not in grouped:
                grouped[a.page_no] = a
            else:
                print(f"Found duplicate annotation for page {a.page_no} in document {doc.md5}")
                cur_value_update_time = grouped[a.page_no].last_changed
                if not cur_value_update_time or cur_value_update_time < a.last_changed:
                    grouped[a.page_no] = a

        completeness = len(grouped) / doc.pages_count
        anno_sum = AnnotationsSummary.get_or_create(doc_md5=doc.md5)
        anno_sum.completeness = completeness

        session = create_session()

        if completeness == 1.0:
            # If the annotation is completed, mark the document as completed
            doc.annotation_completed = True
            anno_sum.missing_pages = None
            docs_to_update.append(doc)
            result = {
                page_no: json.loads(annotation.results)
                for (page_no, annotation)
                in grouped.items()
            }
            # save file locally
            output_file = os.path.join(get_path_in_workdir(Dirs.ANNOTATION_RESULTS), f"{doc.md5}.json")
            with open(output_file, 'w') as file:
                json.dump(result, file, indent=4, sort_keys=True)

            # upload the file to the S3 bucket
            upload_results = upload_files_to_s3(
                [output_file],
                bucket_provider=lambda c: c['yc']['bucket']['annotations_summary'],
                session=session
            )
            anno_sum.result_link = upload_results[output_file]

            classes = set(layout['class'] for page_layouts in result.values() for layout in page_layouts)
            # Check if the document has tables, images or formulas
            if 'table' in classes:
                has_tables = True
            elif 'picture' in classes:
                has_images = True
            elif 'formula' in classes:
                has_formulas = True
        else:
            missing_pages = [str(p) for p in range(1, doc.pages_count + 1) if p not in grouped]
            anno_sum.missing_pages = f"{len(missing_pages)}: {missing_pages.__str__()}"

        anno_sum.has_tables = has_tables
        anno_sum.has_images = has_images
        anno_sum.has_formulas = has_formulas
        anno_summaries_to_update.append(anno_sum)

    # Update the documents and the annotation summaries in the Airtable
    Document.batch_save(docs_to_update)
    AnnotationsSummary.batch_save(anno_summaries_to_update)
    print("Completeness calculation is done!")
