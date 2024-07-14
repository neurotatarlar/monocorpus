import typer

from dispatch import layout_analysis_entry_point, extract_text_entry_point
from annotations_indexer import sync_annotations, calculate_completeness

app = typer.Typer()


@app.command()
def inference(
    force: bool = False
):
    """
    Run PDF Document Layout Analysis service
    """
    layout_analysis_entry_point(force)


@app.command()
def sync():
    """
    Download new and changed annotations from the object storage and update the database
    and calculate the completeness of the annotations
    """
    sync_annotations()
    calculate_completeness()


@app.command()
def extract():
    """
    Extract text from the annotated documents
    """
    extract_text_entry_point()
