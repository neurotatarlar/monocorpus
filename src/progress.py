from rich.progress import Progress, TextColumn, ProgressColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn, TaskProgressColumn, Text, SpinnerColumn, TimeElapsedColumn, FileSizeColumn, TransferSpeedColumn
from rich.live import Live
from rich import print
from rich.progress import Group
from rich.table import Table
from rich.panel import Panel


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
        
        self._main_progress = Progress(
            CheckBoxColumn(),
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
        )

        self.extraction_progress = Progress(
            CheckBoxColumn(),
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            auto_refresh=False,
        )

        self._panel = Panel(
            Group(
                self._main_progress, 
                self.extraction_progress
            ),
            border_style="green"
        )
        progress_table = Table.grid(expand=True)
        progress_table.add_row(self._panel)
        self.live = Live(progress_table)
        
        self._main_progress_active_task = None
        
    def _stop_main_progress(self):
        if self._main_progress_active_task is not None:
            self._main_progress.stop_task(self._main_progress_active_task)
        self._main_progress_active_task = None
        
    def main(self, description):
        self._stop_main_progress()
        
        self._main_progress_active_task = self._main_progress.add_task(
            description=description,
            start=True
        )

    def track_extraction(self, iterable, description="Extraction content of document"):
        self._stop_main_progress()
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
        pass
