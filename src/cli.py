import typer

from dispatch import layout_analysis

app = typer.Typer()


@app.command()
def layout(
        force: bool = False
):
    """
    Run PDF Document Layout Analysis service
    """
    layout_analysis(force)
