from pyairtable.orm import Model, fields as F

from file_utils import read_config

config = read_config()

class Annotation(Model):
    anno_id = F.NumberField("anno_id")
    page_no = F.NumberField("page_no")
    image_link = F.UrlField("image_link")
    anno_md5 = F.TextField("anno_md5")
    results = F.TextField("results")
    doc_md5 = F.TextField("doc_md5")
    last_changed = F.DatetimeField("last_changed")

    class Meta:
        base_id = config['airtable']['base_id']
        table_name = config['airtable']['table']['annotations']
        api_key = config['airtable']['api_key']


class Document(Model):
    # MD5 hash of the file
    md5 = F.TextField("md5")
    # MIME type of the file
    mime_type = F.TextField("mime_type")
    # Names of the document, more than one if there are duplicates
    names = F.TextField("names")
    # Yandex public url of the document
    ya_public_url = F.UrlField("ya_public_url")
    # Yandex public key of the document, used to retrieve temporary download link
    ya_public_key = F.TextField("ya_public_key")
    # Yandex resource id of the document. Together with public key it's used to identify the document
    ya_resource_id = F.TextField("ya_resource_id")
    # Count of pages_slice in the document
    pages_count = F.NumberField("pages_count")
    # Flag to indicate if the document was sent for annotation
    sent_for_annotation = F.CheckboxField("sent_for_annotation")
    # Flag to indicate if the annotation is completed
    annotation_completed = F.CheckboxField("annotation_completed")
    # Flag to indicate if the text was extracted from the document
    text_extracted = F.CheckboxField("text_extracted")

    class Meta:
        base_id = config['airtable']['base_id']
        table_name = config['airtable']['table']['document']
        api_key = config['airtable']['api_key']


class AnnotationsSummary(Model):
    doc_md5 = F.TextField("doc_md5")
    completeness = F.PercentField("completeness")
    result_link = F.UrlField("result_link")
    missing_pages = F.TextField("missing_pages")

    class Meta:
        base_id = config['airtable']['base_id']
        table_name = config['airtable']['table']['annotations_summary']
        api_key = config['airtable']['api_key']

    def get_or_create(doc_md5):
        if not (summary := AnnotationsSummary.first(formula=f"{AnnotationsSummary.doc_md5.field_name}='{doc_md5}'")):
            summary = AnnotationsSummary(doc_md5=doc_md5)
        return summary

