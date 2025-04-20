import typer

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


@app.command()
def extract_content(public_url):
    import dispatch
    dispatch.extract_content(public_url)
