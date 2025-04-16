from rich.progress import Progress, TextColumn, ProgressColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn, TaskProgressColumn, Text, SpinnerColumn, TimeElapsedColumn, FileSizeColumn, TransferSpeedColumn
from rich.live import Live
from rich import print


class CheckBoxColumn(ProgressColumn):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.complete = False

    def render(self, task: "Task") -> Text:
        if task.stop_time:
            return Text("[x]", style="green")
        else:
            return Text("[ ]", style="yellow")

    def update(self, complete):
        self.complete = complete


class ProgressRenderer():

    def __init__(self):

        self.extraction_progress = Progress(
            CheckBoxColumn(),
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            auto_refresh=False,
        )

        self.live = Live(self.extraction_progress)

    def track_extraction(self, iterable, description="Extraction content of document"):
        return ProgressRenderer._track(iterable, description, self.extraction_progress)

    def _track(iterable, description, progress):
        task_id = progress.add_task(
            description=description, total=len(iterable)
        )
        for elem in iterable:
            yield elem
            progress.advance(task_id, 1)

        progress.stop_task(task_id)

    def __enter__(self):
        self.live.start()
        return self

    def __exit__(self, type, value, traceback):
        self.live.__exit__(type, value, traceback)
