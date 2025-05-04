from rich.progress import Progress, TextColumn, ProgressColumn, BarColumn, MofNCompleteColumn, TimeRemainingColumn, TaskProgressColumn, Text, SpinnerColumn, TimeElapsedColumn, FileSizeColumn, TransferSpeedColumn
from rich.live import Live
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
            return Text("==>", style="yellow")

    def update(self, complete):
        self.complete = complete


class ProgressRenderer():

    def __init__(self, context):
        self.context = context
        
        self._main_progress = Progress(
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
            auto_refresh=False,
        )
        self._main_progress_active_task = self._main_progress.add_task(
            description="Starting...",
            start=True,
        )

        self._details_table = Table.grid(expand=False, padding=(0, 1))
        self._operational_progress = Progress(
            CheckBoxColumn(),
            TimeElapsedColumn(),
            TextColumn("{task.description}"),
            auto_refresh=False,
        )

        self._panel = Panel(
            Group(
                self._details_table,
                self._operational_progress,
            ),
            border_style="green"
        )
        progress_table = Table.grid(expand=False)
        progress_table.add_row(self._panel)
        progress_table.add_row(self._main_progress)
        self.live = Live(progress_table)
        self._operational_progress_active_task = None
        self.rendered_table_rows = {}
   
        
    def _stop_operational_progress(self):
        if self._operational_progress_active_task is not None:
            self._operational_progress.stop_task(self._operational_progress_active_task)
        self._operational_progress_active_task = None
        
    def operational(self, description):
        self._stop_operational_progress()
        self._update()
        
        self._operational_progress_active_task = self._operational_progress.add_task(
            description=description,
            start=True
        )
    
    def _update(self, decription="In progress..."):
        table_rows = {}
        if cli_params := self.context.cli_params:
            table_rows = {
                "force": cli_params.force,
                "batch size": cli_params.batch_size,
                "model": cli_params.model,
            }
            if slice:= cli_params.page_slice:
                table_rows["slice"] = f"{slice.start or ''}:{slice.stop or ''}:{slice.step or ''}"
        if self.context.doc.title:
            table_rows["title"] = self.context.doc.title
        if self.context.doc.ya_public_url:
            table_rows["link"] = self.context.doc.ya_public_url
        if self.context.md5:
            # table_rows["md5"] = f"[link=file://{self.context.local_doc_path}]{self.context.md5}[/link]"
            table_rows['md5'] = self.context.md5
        # if self.context.formatted_response_md:
            # table_rows["content"] = f"[link=file://{self.context.formatted_response_md}]{self.context.formatted_response_md}[/link]"
        
        self._main_progress.update(
            self._main_progress_active_task,
            description=decription,
        )
        for key, value in table_rows.items():
            if key not in self.rendered_table_rows:
                self._details_table.add_row(key, f"{value}")
        self.rendered_table_rows.update(table_rows)
        self.live.refresh()

    def track_extraction(self, iterable, description):
        self._stop_operational_progress()
        self._update()
        total = len(iterable)
        task_id = self._operational_progress.add_task(description=f"{description} (0/{total})", total=total)
        for idx, elem  in enumerate(iterable, start=1):
            yield elem
            self._operational_progress.advance(task_id, 1)
            self._operational_progress.update(
                task_id,
                description=f"{description} ({idx}/{total})"
            )
        self._operational_progress.stop_task(task_id)
        if self.context.tokens:
            self._details_table.add_row("TPR", f"{int(sum(self.context.tokens) / len(self.context.tokens))}")

    def _track(self, iterable, description, progress):
        total = len(iterable)
        task_id = progress.add_task(description=f"{description} (0/{total})", total=total)
        for idx, elem  in enumerate(iterable):
            yield elem
            progress.advance(task_id, 1)
            progress.update(
                task_id,
                description=f"{description} ({idx+1}/{total})"
            )

        progress.stop_task(task_id)

    def __enter__(self):
        self.live.start()
        self._update()
        return self

    def __exit__(self, type, value, traceback):
        self._stop_operational_progress()
        if type is KeyboardInterrupt:
            self._update(decription=f"[bold red]Process interrupted by user[/ bold red]")
        self.live.__exit__(type, value, traceback)
