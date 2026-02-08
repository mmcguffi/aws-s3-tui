from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from time import monotonic
from typing import Optional

from rich.style import Style
from rich.measure import Measurement
from rich.segment import Segment
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.strip import Strip
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    Footer,
    Header,
    Input,
    Static,
    TextArea,
    Tree,
)

from .s3 import BucketInfo, ObjectInfo, S3Service
from .s3 import (
    BUCKET_ACCESS_GOOD,
    BUCKET_ACCESS_NO_DOWNLOAD,
    BUCKET_ACCESS_NO_VIEW,
    BUCKET_ACCESS_UNKNOWN,
)


@dataclass(frozen=True)
class NodeInfo:
    profile: Optional[str]
    bucket: str
    prefix: str


@dataclass(frozen=True)
class RowInfo:
    kind: str
    profile: Optional[str] = None
    bucket: Optional[str] = None
    prefix: Optional[str] = None
    key: Optional[str] = None
    size: Optional[int] = None
    last_modified: Optional[datetime] = None


@dataclass(frozen=True)
class PrefixStats:
    dirs: int
    files: int
    total_size: int
    latest_modified: Optional[datetime]


@dataclass(frozen=True)
class DeepStats:
    files: int
    subdirs: int
    total_size: int
    latest_modified: Optional[datetime]
    scanned: int
    truncated: bool


class SplitHandle(Widget):
    def __init__(
        self, orientation: str, before_id: str, after_id: str, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.orientation = orientation
        self.before_id = before_id
        self.after_id = after_id
        self._dragging = False
        self._start_pos = 0
        self._start_before = 0
        self._start_after = 0
        self._total = 0

    def on_mouse_down(self, event: events.MouseDown) -> None:
        before, after = self._targets()
        parent = before.parent
        if parent is None:
            return
        if self.orientation == "vertical":
            self._start_pos = event.screen_x
            self._start_before = before.size.width
            self._start_after = after.size.width
            self._total = max(0, parent.size.width - self.size.width)
        else:
            self._start_pos = event.screen_y
            self._start_before = before.size.height
            self._start_after = after.size.height
            self._total = max(0, parent.size.height - self.size.height)
        self._dragging = True
        self.capture_mouse(True)
        event.stop()

    def on_mouse_move(self, event: events.MouseMove) -> None:
        if not self._dragging:
            return
        delta = (
            event.screen_x - self._start_pos
            if self.orientation == "vertical"
            else event.screen_y - self._start_pos
        )
        before, after = self._targets()
        if self.orientation == "vertical":
            min_before = 20
            min_after = 30
            total = self._total or (self._start_before + self._start_after)
            if total < min_before + min_after:
                min_before = max(1, total // 2)
                min_after = max(1, total - min_before)
            new_before = max(
                min_before, min(total - min_after, self._start_before + delta)
            )
            new_after = total - new_before
            before.styles.width = new_before
            after.styles.width = new_after
        else:
            min_before = 6
            min_after = 6
            total = self._total or (self._start_before + self._start_after)
            if total < min_before + min_after:
                min_before = max(1, total // 2)
                min_after = max(1, total - min_before)
            new_before = max(
                min_before, min(total - min_after, self._start_before + delta)
            )
            new_after = total - new_before
            before.styles.height = new_before
            after.styles.height = new_after
        event.stop()

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if not self._dragging:
            return
        self._dragging = False
        self.capture_mouse(False)
        event.stop()

    def _targets(self):
        before = self.app.query_one(f"#{self.before_id}")
        after = self.app.query_one(f"#{self.after_id}")
        return before, after

    def render(self) -> str:
        if self.orientation == "vertical":
            return "│"
        return "─"


class S3Tree(Tree):
    def on_key(self, event: events.Key) -> None:
        if event.key != "right":
            return
        app = self.app
        if not hasattr(app, "s3_table"):
            return
        app.set_focus(app.s3_table)
        event.stop()


class PreviewTable(DataTable):
    def _get_row_style(self, row_index: int, base_style: Style) -> Style:
        row_style = super()._get_row_style(row_index, base_style)
        if row_index < 0:
            return row_style
        app = self.app
        if not hasattr(app, "_row_keys"):
            return row_style
        if row_index >= len(app._row_keys):
            return row_style
        row_key = app._row_keys[row_index]
        info = app._row_info.get(row_key)
        if info and app._is_selected(info):
            selected_style = self.get_component_styles("datatable--cursor").rich_style
            return row_style + selected_style
        return row_style

    def _render_cell(
        self,
        row_index: int,
        column_index: int,
        base_style: Style,
        width: int,
        cursor: bool = False,
        hover: bool = False,
    ):
        lines = super()._render_cell(
            row_index,
            column_index,
            base_style,
            width,
            cursor=cursor,
            hover=hover,
        )
        padded: list[list[Segment]] = []
        for line in lines:
            if not line:
                padded.append([Segment(" " * width, base_style)])
                continue
            style = line[-1].style or base_style
            strip = Strip(line).adjust_cell_length(width, style)
            padded.append(list(strip))
        return padded

    async def on_click(self, event: events.Click) -> None:
        if event.button != 1:
            return
        row_index = None
        if event.style and event.style.meta:
            row_index = event.style.meta.get("row")
        if row_index is not None and row_index >= 0:
            self.move_cursor(row=row_index, column=self.cursor_column, animate=False)
            self.app.handle_table_selection_click(
                row_index=row_index,
                shift=event.shift,
                toggle=event.meta or event.ctrl,
            )
        if event.chain >= 2:
            await self.app.open_selected_row()
            return
        if event.chain == 1:
            if event.shift or event.meta or event.ctrl:
                return
            await self.app.preview_selected_row()

    def action_cursor_left(self) -> None:
        if hasattr(self.app, "s3_tree"):
            self.app.set_focus(self.app.s3_tree)
            return
        super().action_cursor_left()

    def action_select_cursor(self) -> None:
        info = self.app._row_info_for_cursor()
        if info and info.kind in {"bucket", "prefix", "parent"}:
            self.app.run_worker(self.app.open_selected_row())
            return
        self.app.action_download()


class DownloadDialog(ModalScreen[Optional[str]]):
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]
    CSS = """
    DownloadDialog {
        align: center middle;
    }

    #download-dialog {
        width: 60;
        max-width: 80;
        min-width: 40;
        height: auto;
        min-height: 7;
        margin: 1 2;
        padding: 1 2;
        border: round $panel;
        background: $panel;
        color: $text;
    }

    #download-actions {
        width: 100%;
        content-align: center middle;
        align: center middle;
        margin-top: 1;
        height: auto;
    }

    .download-action {
        width: auto;
        height: auto;
        align: center middle;
        content-align: center middle;
    }

    .download-action-right {
        margin-left: 2;
    }

    .download-hint {
        width: 100%;
        content-align: center middle;
        text-align: center;
        color: $text-muted;
        text-style: dim;
        text-opacity: 60%;
    }

    #download-info {
        width: 100%;
        height: auto;
        margin-top: 1;
        padding: 0 1;
        border: round $panel;
        background: $surface;
        color: $text-muted;
    }

    #download-path {
        width: 100%;
    }

    #download-ok {
        margin-left: 1;
    }
    """

    def __init__(
        self,
        default_path: str,
        label: str = "Download to:",
        info_lines: Optional[list[str]] = None,
    ) -> None:
        super().__init__()
        self._default_path = default_path
        self._label = label
        self._info_lines = info_lines or []

    def compose(self) -> ComposeResult:
        with Vertical(id="download-dialog"):
            yield Static(self._label)
            yield Input(value=self._default_path, id="download-path")
            if self._info_lines:
                yield Static("\n".join(self._info_lines), id="download-info")
            with Horizontal(id="download-actions"):
                with Vertical(classes="download-action"):
                    yield Button("Cancel", id="download-cancel", compact=True)
                    yield Static("Esc", classes="download-hint", markup=False)
                with Vertical(classes="download-action download-action-right"):
                    yield Button("Download", id="download-ok", compact=True)
                    yield Static("Enter", classes="download-hint", markup=False)

    def on_mount(self) -> None:
        self.query_one("#download-path", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "download-cancel":
            self.dismiss(None)
        elif event.button.id == "download-ok":
            self._submit()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "download-path":
            return
        self._submit()

    def _submit(self) -> None:
        value = self.query_one("#download-path", Input).value.strip()
        if not value:
            return
        self.dismiss(value)

    def action_cancel(self) -> None:
        self.dismiss(None)


class RefreshOverlay(ModalScreen[None]):
    BINDINGS = []

    CSS = """
    RefreshOverlay {
        align: center middle;
        background: $background 45%;
    }

    #refresh-overlay-box {
        width: 56;
        max-width: 80;
        min-width: 34;
        height: auto;
        padding: 1 2;
        border: round $panel;
        background: $panel;
        color: $text;
    }

    #refresh-overlay-title {
        width: 100%;
        text-style: bold;
        content-align: center middle;
    }

    #refresh-overlay-detail {
        width: 100%;
        margin-top: 1;
        color: $text-muted;
        content-align: center middle;
    }
    """

    def __init__(self, title: str, detail: str) -> None:
        super().__init__()
        self._title = title
        self._detail = detail

    def compose(self) -> ComposeResult:
        with Vertical(id="refresh-overlay-box"):
            yield Static(self._title, id="refresh-overlay-title")
            yield Static(self._detail, id="refresh-overlay-detail")

    def update_detail(self, detail: str) -> None:
        self._detail = detail
        if not self.is_mounted:
            return
        self.query_one("#refresh-overlay-detail", Static).update(detail)

    def on_key(self, event: events.Key) -> None:
        if event.key != "escape":
            return
        app = self.app
        if hasattr(app, "action_confirm_quit"):
            app.action_confirm_quit()
        event.stop()


PROFILE_DEFAULT_SENTINEL = "__default__"


class ProfileSelectDialog(ModalScreen[Optional[str]]):
    BINDINGS = [("escape", "cancel", "Cancel")]

    CSS = """
    ProfileSelectDialog {
        align: right top;
        background: transparent;
        padding: 3 2 0 0;
    }

    #profile-dialog {
        width: 28;
        max-width: 40;
        min-width: 18;
        height: auto;
        padding: 1;
        border: round $panel;
        background: $panel;
        color: $text;
    }

    #profile-title {
        width: 100%;
        content-align: left middle;
        color: $text-muted;
        margin-bottom: 1;
    }

    .profile-option {
        width: 100%;
        content-align: left middle;
        border: none;
        background: transparent;
        color: $text;
    }
    """

    def __init__(
        self, options: list[tuple[str, str]], current_value: Optional[str]
    ) -> None:
        super().__init__()
        self._options = options
        self._current_value = current_value
        self._button_values: dict[str, str] = {}

    def compose(self) -> ComposeResult:
        with Vertical(id="profile-dialog"):
            yield Static("Choose profile", id="profile-title")
            for index, (label, value) in enumerate(self._options):
                marker = "• " if value == self._current_value else "  "
                button_id = f"profile-option-{index}"
                self._button_values[button_id] = value
                yield Button(f"{marker}{label}", id=button_id, classes="profile-option")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        value = self._button_values.get(event.button.id or "")
        if value is None:
            return
        self.dismiss(value)

    def action_cancel(self) -> None:
        self.dismiss(None)


ONE_MB = 1024**2
HUNDRED_MB = 100 * ONE_MB
ONE_GB = 1024**3
TEN_GB = 10 * ONE_GB
DEEP_SCAN_MAX_KEYS = 50000
ESC_QUIT_WINDOW_SECONDS = 1.0


def format_size(size: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PB"


def size_style(size: int) -> str:
    if size < ONE_MB:
        return "green"
    if size < HUNDRED_MB:
        return "#ffd700"
    if size < ONE_GB:
        return "#ff8c00"
    if size < TEN_GB:
        return "red"
    return "bold red"


class EllipsisCell:
    def __init__(
        self,
        label: str,
        style: str | Style = "",
        justify: str = "left",
    ) -> None:
        self.label = label or ""
        self.style = style
        self.justify = justify

    def __rich_console__(self, console, options):
        if not hasattr(options, "max_width"):
            options = console.options
        width = getattr(options, "max_width", None)
        text = Text(
            self.label, style=self.style, overflow="ellipsis", no_wrap=True, end=""
        )
        if width is None:
            yield text
            return
        text.truncate(width, overflow="ellipsis")
        padding = max(0, width - text.cell_len)
        if self.justify == "right":
            text.pad_left(padding)
        elif self.justify == "center":
            left = padding // 2
            right = padding - left
            if left:
                text.pad_left(left)
            if right:
                text.pad_right(right)
        else:
            text.pad_right(padding)
        yield text

    def __rich_measure__(self, console, options) -> Measurement:
        if not hasattr(options, "max_width"):
            options = console.options
        text = Text(self.label, style=self.style, end="")
        return Measurement.get(console, options, text)


def ellipsis_text(
    label: str, style: str | Style = "", justify: str = "left"
) -> EllipsisCell:
    return EllipsisCell(label, style=style, justify=justify)


def size_cell(label: str, size: Optional[int]) -> EllipsisCell:
    if not label or size is None:
        return ellipsis_text(label)
    return ellipsis_text(label, style=size_style(size), justify="right")


def row_icon(info: RowInfo) -> str:
    if info.kind == "prefix":
        return "📁"
    if info.kind == "error":
        return "⚠"
    return ""


def format_time(value: Optional[datetime]) -> str:
    if not value:
        return ""
    return value.strftime("%Y-%m-%d %H:%M")


def modified_style(value: Optional[datetime]) -> str:
    if not value:
        return ""
    if value.tzinfo is not None:
        now = datetime.now(tz=value.tzinfo)
    else:
        now = datetime.now()
    age_seconds = max(0.0, (now - value).total_seconds())
    age_days = age_seconds / 86400.0
    thresholds = [1, 7, 30, 90, 180, 365]
    colors = [
        "#f0f0f0",
        "#dddddd",
        "#c7c7c7",
        "#b1b1b1",
        "#9b9b9b",
        "#858585",
        "#6f6f6f",
    ]
    index = 0
    for cutoff in thresholds:
        if age_days <= cutoff:
            break
        index += 1
    index = min(index, len(colors) - 1)
    return colors[index]


def modified_cell(label: str, value: Optional[datetime]) -> EllipsisCell:
    if not label or value is None:
        return ellipsis_text(label)
    style = modified_style(value)
    if not style:
        return ellipsis_text(label)
    return ellipsis_text(label, style=style)


def display_segment(full_prefix: str, parent_prefix: str) -> str:
    name = full_prefix[len(parent_prefix) :] if parent_prefix else full_prefix
    return name.strip("/")


def kind_for_row(info: RowInfo) -> str:
    if info.kind in {"prefix", "bucket", "parent"}:
        return "dir"
    if info.kind != "object" or not info.key:
        return ""
    name = info.key.rsplit("/", 1)[-1]
    return kind_from_name(name)


def kind_from_name(name: str) -> str:
    suffixes = PurePosixPath(name).suffixes
    if suffixes and suffixes[-1].lower() == ".gz":
        suffixes = suffixes[:-1]
    if not suffixes:
        return "file"
    ext = suffixes[-1].lstrip(".").lower()
    if not ext:
        return "file"
    mapping = {
        "fa": "fasta",
        "fna": "fasta",
        "fasta": "fasta",
        "ffn": "fasta",
        "faa": "fasta",
        "frn": "fasta",
        "fq": "fastq",
        "fastq": "fastq",
        "bam": "bam",
        "sam": "sam",
        "cram": "cram",
        "txt": "txt",
        "csv": "csv",
        "tsv": "tsv",
        "json": "json",
        "ndjson": "ndjson",
        "parquet": "parquet",
        "vcf": "vcf",
        "bed": "bed",
        "gff": "gff",
        "gff3": "gff3",
        "gtf": "gtf",
    }
    return mapping.get(ext, ext)


class S3Browser(App):
    CSS = """
    #path-bar {
        height: 3;
        padding: 0 1;
        border: round $panel;
        background: $surface;
        color: $text;
        content-align: left middle;
    }

    #path-prefix {
        width: 5;
        content-align: left middle;
        color: $text;
    }

    #path-profile {
        width: auto;
        min-width: 6;
        height: 1;
        padding: 0 1;
        margin-left: 1;
        margin-right: 1;
        content-align: center middle;
        background: $panel;
        color: $text-muted;
        border: none;
    }

    #path-profile:hover {
        background: $surface-lighten-1 20%;
    }

    #path-profile:focus {
        background: $surface-lighten-1 30%;
    }

    #path-profile:disabled {
        color: $text-muted 60%;
    }

    #path-input {
        width: 1fr;
        height: 1;
        background: $panel;
        color: $text;
        border: none;
    }

    #body {
        height: 1fr;
        overflow-y: hidden;
    }

    #s3-tree {
        width: 35%;
        min-width: 24;
        border: round $panel;
    }

    #s3-tree > .tree--highlight-line {
        background: $surface-lighten-1 35%;
    }

    #s3-tree > .tree--cursor {
        text-style: none;
    }

    #s3-tree:focus > .tree--cursor {
        text-style: none;
    }

    #s3-table {
        width: 1fr;
        border: round $panel;
        height: 2fr;
        min-height: 6;
        scrollbar-gutter: stable;
    }

    #s3-table > .datatable--hover {
        background: $surface-lighten-1 35%;
        text-style: none;
    }

    #s3-table > .datatable--cursor {
        text-style: none;
    }

    #s3-table:focus > .datatable--cursor {
        text-style: none;
    }

    #preview {
        width: 1fr;
        height: 1fr;
        border: round $panel;
        background: #2b2f33;
        color: $text;
        min-height: 6;
    }

    #preview-header {
        height: 3;
        padding: 0 1;
        border: round $panel;
        background: $surface;
        color: $text;
        content-align: left middle;
    }

    #preview-content {
        width: 1fr;
        height: 1fr;
        background: #202427;
        color: $text;
        border: none;
    }

    #preview-bar {
        height: 1;
        padding: 0 1;
        background: $surface;
        color: $text;
    }

    #preview-status {
        width: 1fr;
        color: $text-muted;
        content-align: left middle;
    }

    #preview-more {
        width: auto;
        background: $panel;
        color: $text;
    }

    #preview-more.hidden {
        display: none;
    }

    #nav-back {
        margin-left: 1;
        margin-right: 1;
    }

    #nav-back,
    #nav-forward,
    #download {
        height: 1;
        min-width: 3;
        padding: 0 1;
        content-align: center middle;
        border: none;
        background: $panel;
        color: $text;
    }

    #download {
        margin-left: 1;
    }

    #right-pane {
        width: 1fr;
        height: 1fr;
        overflow-y: hidden;
    }

    .split-vertical {
        width: 1;
        background: $panel;
        color: $text-muted;
    }

    .split-vertical:hover {
        background: $accent;
        color: $text;
    }

    .split-horizontal {
        height: 1;
        background: $panel;
        color: $text-muted;
    }

    .split-horizontal:hover {
        background: $accent;
        color: $text;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("escape", "confirm_quit", "Quit x2"),
        ("r", "refresh", "Refresh"),
        ("enter", "open", "Open"),
        ("backspace", "up", "Up"),
        ("alt+left", "back", "Back"),
        ("alt+right", "forward", "Forward"),
        ("ctrl+l", "focus_path", "Path"),
        ("space", "preview", "Preview"),
        ("m", "preview_more", "More/Scan"),
    ]

    def __init__(
        self, profiles: Optional[list[str]] = None, region: Optional[str] = None
    ) -> None:
        super().__init__()
        self.service = S3Service(profiles=profiles, region=region)
        self.buckets: list[BucketInfo] = []
        self.bucket_nodes: dict[tuple[Optional[str], str], object] = {}
        self.bucket_profile_candidates: dict[str, list[Optional[str]]] = {}
        self.prefix_nodes: dict[tuple[Optional[str], str, str], object] = {}
        self.loaded_nodes: set[int] = set()
        self.current_context: Optional[NodeInfo] = None
        self._row_keys: list[object] = []
        self._row_info: dict[object, RowInfo] = {}
        self._load_token = 0
        self._content_token = 0
        self._canonical_path = "s3://"
        self._content_rows: list[tuple[str, str, str, str, RowInfo]] = []
        self._active_filter = ""
        self._pending_created: list[tuple[object, str, tuple]] = []
        self._pending_prev_node: Optional[object] = None
        self._pending_target_node: Optional[object] = None
        self._preview_token = 0
        self._preview_bytes = 4096
        self._preview_key: Optional[RowInfo] = None
        self._preview_content = ""
        self._preview_next_start = 0
        self._preview_total: Optional[int] = None
        self._preview_truncated = False
        self._preview_stats_info: Optional[RowInfo] = None
        self._preview_stats_shallow: Optional[PrefixStats] = None
        self._preview_stats_deep: Optional[DeepStats] = None
        self._sort_column: Optional[str] = None
        self._sort_reverse = False
        self._suppress_filter = False
        self._history: list[Optional[NodeInfo]] = []
        self._history_index = -1
        self._suppress_history_once = False
        self._selected_objects: set[tuple[Optional[str], str, str]] = set()
        self._selection_anchor: Optional[int] = None
        self._showing_selection_summary = False
        self._filter_input_value = ""
        self._col_icon = None
        self._col_name = None
        self._col_kind = None
        self._col_size = None
        self._col_modified = None
        self._quit_escape_deadline = 0.0
        self._sso_reauth_inflight: dict[str, asyncio.Task[bool]] = {}

    def compose(self) -> ComposeResult:
        yield Header()
        with Horizontal(id="path-bar"):
            yield Static("s3://", id="path-prefix")
            yield Input(placeholder="bucket/prefix/", id="path-input")
            yield Button("[-]", id="path-profile", compact=True)
            yield Button("←", id="nav-back", compact=True)
            yield Button("→", id="nav-forward", compact=True)
            yield Button("↓", id="download", compact=True)
        with Horizontal(id="body"):
            yield S3Tree("", id="s3-tree")
            yield SplitHandle(
                "vertical",
                before_id="s3-tree",
                after_id="right-pane",
                classes="split-vertical",
            )
            with Vertical(id="right-pane"):
                yield PreviewTable(id="s3-table")
                yield SplitHandle(
                    "horizontal",
                    before_id="s3-table",
                    after_id="preview",
                    classes="split-horizontal",
                )
                with Vertical(id="preview"):
                    yield Static("", id="preview-header")
                    yield TextArea(
                        "",
                        id="preview-content",
                        read_only=True,
                        show_cursor=False,
                        soft_wrap=True,
                        placeholder="Press Space to preview a file or folder",
                    )
                with Horizontal(id="preview-bar"):
                    yield Static("", id="preview-status")
                    yield Button("More", id="preview-more", compact=True)
        yield Footer()

    async def on_mount(self) -> None:
        self.s3_tree = self.query_one("#s3-tree", Tree)
        self.s3_tree.show_root = False
        self.s3_table = self.query_one("#s3-table", DataTable)
        self.path_input = self.query_one("#path-input", Input)
        self.path_profile = self.query_one("#path-profile", Button)
        self.nav_back = self.query_one("#nav-back", Button)
        self.nav_forward = self.query_one("#nav-forward", Button)
        self.download_button = self.query_one("#download", Button)
        self.preview_header = self.query_one("#preview-header", Static)
        self.preview = self.query_one("#preview-content", TextArea)
        self.preview_status = self.query_one("#preview-status", Static)
        self.preview_more = self.query_one("#preview-more", Button)
        self._set_path_value("s3://", canonical="s3://", suppress_filter=True)
        self._set_profile_indicator(None)
        self._col_icon = self.s3_table.add_column("", width=2)
        (
            self._col_name,
            self._col_kind,
            self._col_size,
            self._col_modified,
        ) = self.s3_table.add_columns("Name", "Kind", "Size", "Modified")
        self._update_sort_headers()
        self.s3_table.cursor_type = "row"
        self.s3_table.zebra_stripes = True
        self.s3_tree.root.expand()
        self.set_focus(self.s3_tree)
        self._sync_nav_buttons()
        self._resize_table_columns()
        await self._ensure_sso_logins()
        await self.refresh_buckets()

    async def _ensure_sso_logins(self) -> None:
        if not hasattr(self.service, "sso_login_targets"):
            return
        try:
            targets = self.service.sso_login_targets()
        except Exception as exc:
            self.notify(f"SSO preflight failed: {exc}", severity="warning")
            return
        for profile in targets:
            self.notify(
                f"SSO login required for profile '{profile}'. Opening browser...",
                severity="warning",
            )
            await self._run_sso_login(profile)

    async def _run_sso_login(self, profile: str) -> bool:
        try:
            process = await asyncio.create_subprocess_exec(
                "aws",
                "sso",
                "login",
                "--profile",
                profile,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError:
            self.notify(
                "AWS CLI not found; cannot run `aws sso login`.", severity="error"
            )
            return False
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            return True
        message = stderr.decode("utf-8", errors="replace").strip()
        if not message:
            message = stdout.decode("utf-8", errors="replace").strip()
        if not message:
            message = f"aws sso login failed for profile '{profile}'."
        self.notify(message, severity="error")
        return False

    def _profile_label(self, profile: Optional[str]) -> str:
        return profile or "default"

    def _is_sso_expired_error(self, exc: Exception) -> bool:
        if exc is None:
            return False
        text = f"{type(exc).__name__}: {exc}".lower()
        markers = [
            "unauthorizedssotokenerror",
            "sso session",
            "sso token",
            "token has expired",
            "token is expired",
            "expiredtoken",
            "the sso session associated with this profile has expired",
            "error loading sso token",
            "run aws sso login",
            "aws sso login",
        ]
        return any(marker in text for marker in markers)

    async def _reauth_sso_profile(self, profile: Optional[str]) -> bool:
        profile_name = self._profile_label(profile)
        inflight = self._sso_reauth_inflight.get(profile_name)
        if inflight is None:

            async def do_login() -> bool:
                self.notify(
                    f"SSO token expired for '{profile_name}'. Running aws sso login...",
                    severity="warning",
                )
                ok = await self._run_sso_login(profile_name)
                if ok:
                    self.notify(
                        f"SSO login refreshed for '{profile_name}'.",
                        severity="information",
                    )
                return ok

            inflight = asyncio.create_task(do_login())
            self._sso_reauth_inflight[profile_name] = inflight
        try:
            return await inflight
        finally:
            active = self._sso_reauth_inflight.get(profile_name)
            if active is inflight and inflight.done():
                self._sso_reauth_inflight.pop(profile_name, None)

    async def _call_with_sso_retry(
        self,
        profile: Optional[str],
        operation,
        *args,
        **kwargs,
    ):
        try:
            return await operation(*args, **kwargs)
        except Exception as exc:
            if not self._is_sso_expired_error(exc):
                raise
            relogged = await self._reauth_sso_profile(profile)
            if not relogged:
                raise
            return await operation(*args, **kwargs)

    def on_resize(self, event: events.Resize) -> None:
        self._resize_table_columns()

    def _resize_table_columns(self) -> None:
        if not hasattr(self, "s3_table"):
            return
        if not self._col_name or not self._col_kind:
            return
        table = self.s3_table
        total_width = table.content_region.width
        if total_width <= 0:
            return
        padding = table.cell_padding
        icon_width = 2
        kind_content = (
            table.columns[self._col_kind].content_width if self._col_kind else 6
        )
        kind_base = max(4, min(kind_content, 8))
        size_base = max(
            10,
            table.columns[self._col_size].content_width if self._col_size else 10,
        )
        modified_base = max(
            16,
            table.columns[self._col_modified].content_width
            if self._col_modified
            else 16,
        )
        name_base = max(
            8,
            table.columns[self._col_name].content_width if self._col_name else 8,
        )
        available_content = total_width - (2 * padding * 5)
        if available_content <= 0:
            return
        content_total = icon_width + size_base + modified_base + name_base + kind_base
        if content_total >= available_content:
            name_width = max(
                1,
                available_content
                - (icon_width + size_base + modified_base + kind_base),
            )
            size_width = size_base
            modified_width = modified_base
            kind_width = kind_base
        else:
            extra = available_content - content_total
            size_extra = int(extra * 0.08)
            kind_extra = int(extra * 0.04)
            modified_extra = int(extra * 0.2)
            name_extra = extra - size_extra - kind_extra - modified_extra
            size_width = size_base + size_extra
            modified_width = modified_base + modified_extra
            kind_width = kind_base + kind_extra
            name_width = name_base + name_extra

        def apply_width(column_key, width: int) -> None:
            column = table.columns.get(column_key)
            if not column:
                return
            column.width = width
            column.auto_width = False

        apply_width(self._col_icon, icon_width)
        apply_width(self._col_kind, kind_width)
        apply_width(self._col_size, size_width)
        apply_width(self._col_modified, modified_width)
        apply_width(self._col_name, name_width)
        table._require_update_dimensions = True
        table.refresh(layout=True)

    def _bucket_label(self, bucket: BucketInfo) -> Text:
        profile_label = bucket.profile or "default"
        style = self._bucket_name_style(bucket.access)
        label = Text(bucket.name, style=style)
        label.append(f" [{profile_label}]", style="dim")
        return label

    def _bucket_name_style(self, access: str) -> str:
        if access == BUCKET_ACCESS_NO_VIEW:
            return "bold red"
        if access == BUCKET_ACCESS_NO_DOWNLOAD:
            return "bold #ff8c00"
        if access == BUCKET_ACCESS_GOOD:
            return "bold #2f80ed"
        return "bold #2f80ed"

    def _bucket_access_for_name(self, bucket: Optional[str]) -> str:
        if not bucket:
            return BUCKET_ACCESS_UNKNOWN
        for info in self.buckets:
            if info.name == bucket:
                return info.access
        return BUCKET_ACCESS_UNKNOWN

    def _set_profile_indicator(
        self, profile: Optional[str], bucket: Optional[str] = None
    ) -> None:
        if not hasattr(self, "path_profile"):
            return
        if not bucket:
            self.path_profile.label = "[-]"
            self.path_profile.disabled = True
            self.path_profile.styles.color = "#8a8a8a"
            return
        access = self._bucket_access_for_name(bucket)
        profile_style = self._bucket_name_style(access).replace("bold ", "")
        self.path_profile.disabled = False
        self.path_profile.label = f"[{profile or 'default'}]"
        self.path_profile.styles.color = profile_style

    def _bucket_access_level(self, access: str) -> int:
        if access == BUCKET_ACCESS_GOOD:
            return 2
        if access == BUCKET_ACCESS_NO_DOWNLOAD:
            return 1
        return 0

    async def _resolve_profile_for_bucket_access(
        self, bucket: str, current_profile: Optional[str]
    ) -> tuple[Optional[str], str]:
        bucket_access = getattr(self.service, "bucket_access", None)
        if not callable(bucket_access):
            return current_profile, self._bucket_access_for_name(bucket)

        candidates = self._profile_candidates_for_bucket(bucket)
        if current_profile not in candidates:
            candidates.insert(0, current_profile)
        candidates = [profile for profile in candidates if profile is not None] + (
            [None] if None in candidates else []
        )

        profile_order: dict[Optional[str], int] = {}
        if hasattr(self.service, "profiles"):
            profile_order = {
                profile: index for index, profile in enumerate(getattr(self.service, "profiles"))
            }

        async def probe_access(profile: Optional[str]) -> str:
            try:
                result = await self._call_with_sso_retry(
                    profile,
                    bucket_access,
                    profile,
                    bucket,
                )
            except Exception:
                return BUCKET_ACCESS_NO_VIEW
            if not isinstance(result, str):
                return BUCKET_ACCESS_NO_VIEW
            normalized = result.strip().lower()
            if normalized in {
                BUCKET_ACCESS_GOOD,
                BUCKET_ACCESS_NO_DOWNLOAD,
                BUCKET_ACCESS_NO_VIEW,
            }:
                return normalized
            return BUCKET_ACCESS_NO_VIEW

        checks = [probe_access(profile) for profile in candidates]
        results = await asyncio.gather(*checks)

        best_profile = current_profile
        best_access = self._bucket_access_for_name(bucket)
        best_key = (
            self._bucket_access_level(best_access),
            1 if best_profile is not None else 0,
            -profile_order.get(best_profile, len(profile_order)),
        )

        for profile, access in zip(candidates, results):
            key = (
                self._bucket_access_level(access),
                1 if profile is not None else 0,
                -profile_order.get(profile, len(profile_order)),
            )
            if key > best_key:
                best_key = key
                best_profile = profile
                best_access = access

        return best_profile, best_access

    async def action_refresh(self) -> None:
        await self.refresh_buckets()

    def action_confirm_quit(self) -> None:
        now = monotonic()
        if now <= self._quit_escape_deadline:
            self._quit_escape_deadline = 0.0
            self.exit()
            return
        self._quit_escape_deadline = now + ESC_QUIT_WINDOW_SECONDS
        self.notify(
            "Press Esc again within 1 second to quit.",
            severity="warning",
        )

    async def action_open(self) -> None:
        if self.focused is self.s3_table:
            await self.preview_selected_row()
            return
        if self.focused is self.s3_tree:
            node = self.s3_tree.cursor_node
            if node and node.allow_expand:
                node.toggle()

    async def action_preview(self) -> None:
        if self.focused is not self.s3_table:
            return
        await self.preview_selected_row()

    def action_download(self) -> None:
        self.run_worker(self._download_flow(), exclusive=True)

    async def _download_flow(self) -> None:
        selected = self._selected_object_infos()
        if not selected:
            row_key = self._row_key_for_cursor()
            if row_key is None:
                self.notify("Select a file or folder to download.", severity="warning")
                return
            info = self._row_info.get(row_key)
            if not info:
                self.notify("Select a file or folder to download.", severity="warning")
                return
            if info.kind == "prefix":
                await self._download_prefix(info)
                return
            if info.kind != "object" or not info.key or not info.bucket:
                self.notify("Select a file or folder to download.", severity="warning")
                return
            selected = [info]

        if len(selected) >= 2:
            default_dir = str(Path.home() / "Downloads")
            info_lines = self._download_info_lines(selected)
            target = await self.push_screen_wait(
                DownloadDialog(
                    default_dir, label="Download directory:", info_lines=info_lines
                )
            )
            if not target:
                return
            directory = self._resolve_download_dir(target)
            self.notify(f"Downloading {len(selected)} files...", severity="information")
            try:
                for info in selected:
                    destination = str(directory / (Path(info.key).name or "download"))
                    await self._call_with_sso_retry(
                        info.profile,
                        self.service.download_object,
                        info.profile,
                        info.bucket,
                        info.key,
                        destination,
                    )
            except Exception as exc:
                self.notify(f"{exc}", severity="error")
                return
            self.notify(f"Downloaded to {directory}", severity="information")
            return

        info = selected[0]
        default_name = str(Path.home() / (Path(info.key).name or "download"))
        info_lines = self._download_info_lines([info])
        target = await self.push_screen_wait(
            DownloadDialog(default_name, info_lines=info_lines)
        )
        if not target:
            return
        destination = self._resolve_download_path(target, info)
        self.notify("Downloading...", severity="information")
        try:
            await self._call_with_sso_retry(
                info.profile,
                self.service.download_object,
                info.profile,
                info.bucket,
                info.key,
                destination,
            )
        except Exception as exc:
            self.notify(f"{exc}", severity="error")
            return
        self.notify(f"Downloaded to {destination}", severity="information")

    async def _download_prefix(self, info: RowInfo) -> None:
        if not info.bucket or info.prefix is None:
            self.notify("Select a folder to download.", severity="warning")
            return
        default_dir = str(
            Path.home() / "Downloads" / self._prefix_download_name(info)
        )
        info_lines = self._download_prefix_info_lines(info)
        target = await self.push_screen_wait(
            DownloadDialog(
                default_dir, label="Download folder to:", info_lines=info_lines
            )
        )
        if not target:
            return
        target_dir = self._resolve_prefix_download_dir(target, info)
        self.notify("Listing files...", severity="information")
        try:
            objects = await self._call_with_sso_retry(
                info.profile,
                self.service.list_objects_recursive,
                info.profile,
                info.bucket,
                info.prefix,
            )
        except Exception as exc:
            self.notify(f"{exc}", severity="error")
            return
        if not objects:
            self.notify("No files to download.", severity="warning")
            return
        self.notify(
            f"Downloading {len(objects)} files...", severity="information"
        )
        base_prefix = info.prefix or ""
        if base_prefix and not base_prefix.endswith("/"):
            base_prefix = f"{base_prefix}/"
        try:
            for obj in objects:
                if base_prefix and obj.key.startswith(base_prefix):
                    relative = obj.key[len(base_prefix) :]
                else:
                    relative = obj.key
                destination = str(target_dir / relative)
                await self._call_with_sso_retry(
                    info.profile,
                    self.service.download_object,
                    info.profile,
                    info.bucket,
                    obj.key,
                    destination,
                )
        except Exception as exc:
            self.notify(f"{exc}", severity="error")
            return
        self.notify(f"Downloaded to {target_dir}", severity="information")

    async def action_preview_more(self) -> None:
        if (
            self._preview_stats_info
            and self._preview_stats_shallow
            and self._preview_stats_deep is None
        ):
            if self.preview_more.disabled:
                return
            await self._load_deep_prefix_stats()
            return
        if not self._preview_key or not self._preview_truncated:
            return
        await self._load_more_preview()

    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "preview-more":
            await self.action_preview_more()
            return
        if event.button.id == "path-profile":
            await self.action_choose_profile()
            return
        if event.button.id == "nav-back":
            self.action_back()
            return
        if event.button.id == "nav-forward":
            self.action_forward()
            return
        if event.button.id == "download":
            self.action_download()
            return

    def action_focus_path(self) -> None:
        self.set_focus(self.path_input)
        if hasattr(self.path_input, "select_all"):
            self.path_input.select_all()

    async def action_choose_profile(self) -> None:
        if not self.current_context or not self.current_context.bucket:
            self.notify("Open a bucket to choose profile.", severity="warning")
            return
        bucket = self.current_context.bucket
        current_profile = self._profile_for_bucket(bucket)
        options: list[tuple[str, str]] = []
        seen: set[str] = set()
        for profile in self._profile_candidates_for_bucket(bucket):
            value = PROFILE_DEFAULT_SENTINEL if profile is None else profile
            if value in seen:
                continue
            seen.add(value)
            label = profile or "default"
            options.append((label, value))
        if not options:
            self.notify("No profiles available.", severity="warning")
            return
        current_value = (
            PROFILE_DEFAULT_SENTINEL if current_profile is None else current_profile
        )
        selected = await self.push_screen_wait(
            ProfileSelectDialog(options, current_value=current_value)
        )
        if not selected:
            return
        profile = None if selected == PROFILE_DEFAULT_SENTINEL else selected
        if profile == current_profile:
            return

        access = self._bucket_access_for_name(bucket)
        bucket_access = getattr(self.service, "bucket_access", None)
        if callable(bucket_access):
            try:
                access = await self._call_with_sso_retry(
                    profile,
                    bucket_access,
                    profile,
                    bucket,
                )
            except Exception:
                access = BUCKET_ACCESS_NO_VIEW

        node = self.s3_tree.cursor_node
        if node is None:
            node = self.bucket_nodes.get((current_profile, bucket))
        if node is None:
            for (_profile, name), candidate in self.bucket_nodes.items():
                if name == bucket:
                    node = candidate
                    break

        if node is not None:
            self._switch_bucket_profile(
                bucket,
                current_profile,
                profile,
                node,
                new_access=access,
            )
        else:
            updated: list[BucketInfo] = []
            for info in self.buckets:
                if info.name == bucket:
                    updated.append(
                        BucketInfo(name=info.name, profile=profile, access=access)
                    )
                else:
                    updated.append(info)
            self.buckets = updated

        save_bucket_cache = getattr(self.service, "save_bucket_cache", None)
        if callable(save_bucket_cache):
            await asyncio.to_thread(save_bucket_cache, self.buckets)

        self._set_profile_indicator(profile, bucket)
        self.navigate_to(profile, bucket, self.current_context.prefix)

    def action_back(self) -> None:
        if self._history_index <= 0:
            return
        self._history_index -= 1
        self._sync_nav_buttons()
        self._suppress_history_once = True
        self._navigate_history(self._history[self._history_index])

    def action_forward(self) -> None:
        if self._history_index >= len(self._history) - 1:
            return
        self._history_index += 1
        self._sync_nav_buttons()
        self._suppress_history_once = True
        self._navigate_history(self._history[self._history_index])

    async def action_up(self) -> None:
        if not self.current_context:
            return
        if not self.current_context.prefix:
            self.s3_tree.select_node(self.s3_tree.root)
            return
        parent_prefix = self._parent_prefix(self.current_context.prefix)
        self.navigate_to(
            self.current_context.profile, self.current_context.bucket, parent_prefix
        )

    async def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        node = event.node
        if node is self.s3_tree.root or node.data is None:
            self.current_context = None
            self._set_path_value("s3://", canonical="s3://", suppress_filter=True)
            self.show_bucket_list()
            return
        info: NodeInfo = node.data
        await self.show_prefix(node, info)

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.data_table is not self.s3_table:
            return
        if self._filter_input_value:
            return
        info = self._row_info.get(event.row_key)
        if not info:
            return
        selection_path = self._path_for_row(info)
        if selection_path:
            self._set_path_value(selection_path, suppress_filter=True)

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        if event.data_table is not self.s3_table:
            return
        sort_map = {
            self._col_name: "name",
            self._col_kind: "kind",
            self._col_size: "size",
            self._col_modified: "modified",
        }
        sort_column = sort_map.get(event.column_key)
        if not sort_column:
            return
        current_info = self._row_info_for_cursor()
        if self._sort_column == sort_column:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_column = sort_column
            self._sort_reverse = False
        self._update_sort_headers()
        self._apply_filter(self._active_filter, force=True)
        if current_info:
            self._restore_cursor_info(current_info)

    async def on_tree_node_expanded(self, event: Tree.NodeExpanded) -> None:
        node = event.node
        if node is self.s3_tree.root or node.data is None:
            return
        if node.id in self.loaded_nodes:
            return
        info: NodeInfo = node.data
        try:
            prefixes = await self._call_with_sso_retry(
                info.profile,
                self.service.list_prefixes,
                info.profile,
                info.bucket,
                info.prefix,
            )
        except Exception as exc:
            node.allow_expand = False
            self.notify(f"{exc}", severity="error")
            return
        self.loaded_nodes.add(node.id)
        self._sync_prefix_children(node, info, prefixes)

    def _collect_bucket_profile_candidates(
        self, buckets: list[BucketInfo]
    ) -> dict[str, list[Optional[str]]]:
        candidates: dict[str, list[Optional[str]]] = {}
        for bucket in buckets:
            values = candidates.setdefault(bucket.name, [])
            if bucket.profile not in values:
                values.append(bucket.profile)
        return candidates

    def _render_bucket_nodes(self, buckets: list[BucketInfo]) -> None:
        self.s3_tree.clear()
        self.bucket_nodes.clear()
        self.prefix_nodes.clear()
        self.loaded_nodes.clear()
        self.buckets = sorted(buckets, key=lambda b: b.name.lower())
        for bucket in self.buckets:
            values = self.bucket_profile_candidates.setdefault(bucket.name, [])
            if bucket.profile not in values:
                values.append(bucket.profile)
        for bucket in self.buckets:
            node = self.s3_tree.root.add(
                self._bucket_label(bucket),
                data=NodeInfo(profile=bucket.profile, bucket=bucket.name, prefix=""),
                allow_expand=True,
            )
            self.bucket_nodes[(bucket.profile, bucket.name)] = node
        self.s3_tree.root.expand()
        self.s3_tree.select_node(self.s3_tree.root)

    async def refresh_buckets(self) -> None:
        self._load_token += 1
        token = self._load_token
        overlay = RefreshOverlay(
            "Refreshing Buckets",
            "Listing buckets across configured profiles...",
        )
        self.push_screen(overlay)
        await asyncio.sleep(0)
        self.s3_tree.clear()
        self.bucket_nodes.clear()
        self.bucket_profile_candidates.clear()
        self.prefix_nodes.clear()
        self.loaded_nodes.clear()
        self.current_context = None
        self._clear_table()
        self._content_rows = []
        self._active_filter = ""
        self._clear_selection()
        self._filter_input_value = ""
        self._history = []
        self._history_index = -1
        self._suppress_history_once = False
        self._sync_nav_buttons()
        self._set_path_value("s3://", canonical="s3://", suppress_filter=True)
        self._set_profile_indicator(None)
        self.path_input.placeholder = "Loading buckets..."
        self.s3_tree.root.expand()
        try:
            cached: list[BucketInfo] = []
            load_bucket_cache = getattr(self.service, "load_bucket_cache", None)
            if callable(load_bucket_cache):
                cached = await asyncio.to_thread(load_bucket_cache)
            if token != self._load_token:
                return
            has_cached = bool(cached)
            if has_cached:
                self.bucket_profile_candidates = self._collect_bucket_profile_candidates(
                    cached
                )
                self.path_input.placeholder = "Refreshing buckets..."
                self._render_bucket_nodes(cached)

            overlay.update_detail("Listing buckets across configured profiles...")
            buckets, errors = await self.service.list_buckets_all()
            if errors:
                retry_profiles: list[Optional[str]] = []
                seen_profiles: set[str] = set()
                for profile, error in errors:
                    if not self._is_sso_expired_error(error):
                        continue
                    label = self._profile_label(profile)
                    if label in seen_profiles:
                        continue
                    seen_profiles.add(label)
                    retry_profiles.append(profile)
                if retry_profiles:
                    for profile in retry_profiles:
                        overlay.update_detail(
                            f"SSO expired for '{self._profile_label(profile)}'. Re-authenticating..."
                        )
                        await self._reauth_sso_profile(profile)
                    overlay.update_detail(
                        "Retrying bucket listing after SSO re-authentication..."
                    )
                    buckets, errors = await self.service.list_buckets_all()
            if token != self._load_token:
                return
            self.bucket_profile_candidates = self._collect_bucket_profile_candidates(
                buckets
            )

            overlay.update_detail("Testing permissions to choose best profile per bucket...")
            buckets = await self.service.select_best_bucket_profiles(buckets)
            if token != self._load_token:
                return

            if buckets:
                save_bucket_cache = getattr(self.service, "save_bucket_cache", None)
                if callable(save_bucket_cache):
                    await asyncio.to_thread(save_bucket_cache, buckets)

            if not buckets and has_cached and errors:
                self.path_input.placeholder = "bucket/prefix/ (cached)"
                self.notify(
                    "Using cached buckets because live refresh failed.",
                    severity="warning",
                )
                if errors:
                    failed = ", ".join(
                        (profile or "default") for profile, _exc in errors[:3]
                    )
                    extra = ""
                    if len(errors) > 3:
                        extra = f" (+{len(errors) - 3} more)"
                    self.notify(
                        f"Some credentials could not list buckets: {failed}{extra}",
                        severity="warning",
                    )
                return

            self.path_input.placeholder = "bucket/prefix/"
            self._render_bucket_nodes(buckets)
            if errors:
                failed = ", ".join(
                    (profile or "default") for profile, _exc in errors[:3]
                )
                extra = ""
                if len(errors) > 3:
                    extra = f" (+{len(errors) - 3} more)"
                self.notify(
                    f"Some credentials could not list buckets: {failed}{extra}",
                    severity="warning",
                )
        finally:
            try:
                if overlay.is_mounted:
                    overlay.dismiss(None)
            except Exception:
                pass

    def _profile_candidates_for_bucket(self, bucket: str) -> list[Optional[str]]:
        candidates = list(self.bucket_profile_candidates.get(bucket, []))
        if not candidates:
            profile = self._profile_for_bucket(bucket)
            if profile not in candidates:
                candidates.append(profile)
        profile_order = {}
        if hasattr(self.service, "profiles"):
            service_profiles = list(getattr(self.service, "profiles"))
            profile_order = {
                profile: index for index, profile in enumerate(service_profiles)
            }
            for profile in service_profiles:
                if profile not in candidates:
                    candidates.append(profile)
        candidates = sorted(
            candidates,
            key=lambda profile: (
                profile is None,
                profile_order.get(profile, len(profile_order)),
            ),
        )
        return candidates

    def _switch_bucket_profile(
        self,
        bucket: str,
        old_profile: Optional[str],
        new_profile: Optional[str],
        current_node: object,
        new_access: Optional[str] = None,
    ) -> None:
        existing_access = self._bucket_access_for_name(bucket)
        chosen_access = new_access or existing_access
        if old_profile == new_profile:
            bucket_node = self.bucket_nodes.get((new_profile, bucket))
            if bucket_node is not None:
                try:
                    bucket_node.data = NodeInfo(
                        profile=new_profile, bucket=bucket, prefix=""
                    )
                except Exception:
                    pass
                try:
                    label = self._bucket_label(
                        BucketInfo(name=bucket, profile=new_profile, access=chosen_access)
                    )
                    bucket_node.set_label(label)
                except Exception:
                    pass
            updated: list[BucketInfo] = []
            for info in self.buckets:
                if info.name == bucket:
                    updated.append(
                        BucketInfo(
                            name=info.name,
                            profile=info.profile,
                            access=chosen_access,
                        )
                    )
                else:
                    updated.append(info)
            self.buckets = updated
            return
        old_bucket_key = (old_profile, bucket)
        source_profile = old_profile
        bucket_node = self.bucket_nodes.get(old_bucket_key)
        if bucket_node is None:
            for (profile_key, name), candidate in self.bucket_nodes.items():
                if name != bucket:
                    continue
                source_profile = profile_key
                bucket_node = candidate
                break
        if bucket_node is not None:
            if source_profile is not None or old_bucket_key in self.bucket_nodes:
                self.bucket_nodes.pop((source_profile, bucket), None)
            self.bucket_nodes.pop(old_bucket_key, None)
            self.bucket_nodes[(new_profile, bucket)] = bucket_node
            try:
                bucket_node.data = NodeInfo(profile=new_profile, bucket=bucket, prefix="")
            except Exception:
                pass
            try:
                label = self._bucket_label(
                    BucketInfo(name=bucket, profile=new_profile, access=chosen_access)
                )
                bucket_node.set_label(label)
            except Exception:
                pass
        else:
            replacement = self.bucket_nodes.pop(old_bucket_key, None)
            if replacement is not None:
                self.bucket_nodes[(new_profile, bucket)] = replacement

        current_data = getattr(current_node, "data", None)
        if isinstance(current_data, NodeInfo):
            if current_data.bucket == bucket and current_data.profile == old_profile:
                try:
                    current_node.data = NodeInfo(
                        profile=new_profile,
                        bucket=current_data.bucket,
                        prefix=current_data.prefix,
                    )
                except Exception:
                    pass

        updated: list[BucketInfo] = []
        for info in self.buckets:
            if info.name == bucket:
                updated.append(
                    BucketInfo(
                        name=info.name,
                        profile=new_profile,
                        access=chosen_access,
                    )
                )
            else:
                updated.append(info)
        self.buckets = updated

        profile_to_replace = source_profile if bucket_node is not None else old_profile
        prefix_updates: list[
            tuple[
                tuple[Optional[str], str, str],
                tuple[Optional[str], str, str],
                object,
            ]
        ] = []
        for key, prefix_node in self.prefix_nodes.items():
            profile, key_bucket, prefix = key
            if key_bucket != bucket or profile != profile_to_replace:
                continue
            new_key = (new_profile, bucket, prefix)
            prefix_updates.append((key, new_key, prefix_node))
        for old_key, new_key, prefix_node in prefix_updates:
            self.prefix_nodes.pop(old_key, None)
            self.prefix_nodes[new_key] = prefix_node
            data = getattr(prefix_node, "data", None)
            if isinstance(data, NodeInfo):
                try:
                    prefix_node.data = NodeInfo(
                        profile=new_profile,
                        bucket=data.bucket,
                        prefix=data.prefix,
                    )
                except Exception:
                    pass

        candidates = self.bucket_profile_candidates.setdefault(bucket, [])
        if new_profile not in candidates:
            candidates.append(new_profile)

    async def _try_bucket_profile_fallback(
        self, info: NodeInfo, node: object
    ) -> tuple[Optional[tuple[NodeInfo, list[str], list[ObjectInfo], bool]], list[Optional[str]]]:
        candidates = self._profile_candidates_for_bucket(info.bucket)
        attempted: list[Optional[str]] = []
        for profile in candidates:
            if profile == info.profile:
                continue
            attempted.append(profile)
            try:
                prefixes, objects, has_any = await self._call_with_sso_retry(
                    profile,
                    self.service.list_prefixes_and_objects,
                    profile,
                    info.bucket,
                    info.prefix,
                )
            except Exception:
                continue
            new_info = NodeInfo(profile=profile, bucket=info.bucket, prefix=info.prefix)
            access = BUCKET_ACCESS_GOOD
            bucket_access = getattr(self.service, "bucket_access", None)
            if callable(bucket_access):
                try:
                    access = await self._call_with_sso_retry(
                        profile,
                        bucket_access,
                        profile,
                        info.bucket,
                    )
                except Exception:
                    access = BUCKET_ACCESS_GOOD
            self._switch_bucket_profile(
                info.bucket,
                info.profile,
                profile,
                node,
                new_access=access,
            )
            self.current_context = new_info
            self._set_profile_indicator(new_info.profile, new_info.bucket)
            self.notify(
                f"Using profile '{profile or 'default'}' for bucket '{info.bucket}'.",
                severity="warning",
            )
            save_bucket_cache = getattr(self.service, "save_bucket_cache", None)
            if callable(save_bucket_cache):
                await asyncio.to_thread(save_bucket_cache, self.buckets)
            return (new_info, prefixes, objects, has_any), attempted
        return None, attempted

    async def show_prefix(self, node, info: NodeInfo) -> None:
        selected_profile = self._profile_for_bucket(info.bucket)
        if selected_profile != info.profile:
            info = NodeInfo(
                profile=selected_profile,
                bucket=info.bucket,
                prefix=info.prefix,
            )
            try:
                node.data = info
            except Exception:
                pass
        resolved_profile, resolved_access = await self._resolve_profile_for_bucket_access(
            info.bucket,
            info.profile,
        )
        if (
            resolved_profile != info.profile
            or self._bucket_access_for_name(info.bucket) != resolved_access
        ):
            self._switch_bucket_profile(
                info.bucket,
                info.profile,
                resolved_profile,
                node,
                new_access=resolved_access,
            )
            info = NodeInfo(
                profile=resolved_profile,
                bucket=info.bucket,
                prefix=info.prefix,
            )
            try:
                node.data = info
            except Exception:
                pass
        self.current_context = info
        self._set_profile_indicator(info.profile, info.bucket)
        self._preview_token += 1
        self._clear_selection()
        self._filter_input_value = ""
        suppress_history = self._consume_history_suppression()
        self._content_token += 1
        token = self._content_token
        path = f"s3://{info.bucket}"
        if info.prefix:
            path = f"{path}/{info.prefix}"
        canonical = path if path.endswith("/") else f"{path}/"
        self._set_path_value(path, canonical=canonical, suppress_filter=True)
        self._clear_table()
        self.s3_table.add_row("", "Loading...", "", "", "")
        try:
            prefixes, objects, has_any = await self._call_with_sso_retry(
                info.profile,
                self.service.list_prefixes_and_objects,
                info.profile,
                info.bucket,
                info.prefix,
            )
        except Exception as exc:
            fallback, attempted = await self._try_bucket_profile_fallback(info, node)
            if fallback is not None:
                info, prefixes, objects, has_any = fallback
            else:
                if self._pending_target_node is node and self._pending_created:
                    prev_node = self._pending_prev_node
                    self._remove_pending_nodes()
                    self._clear_pending()
                    if prev_node is not None:
                        self.s3_tree.select_node(prev_node)
                    else:
                        self.s3_tree.select_node(self.s3_tree.root)
                    self.notify(f"{exc}", severity="error")
                    return
                tried = [info.profile, *attempted]
                tried_text = ", ".join(profile or "default" for profile in tried)
                self._clear_table()
                self._content_rows = [
                    (
                        f"Access denied or unavailable ({tried_text})",
                        "",
                        "",
                        "",
                        RowInfo(kind="error"),
                    )
                ]
                self._active_filter = ""
                self._add_row(
                    f"Access denied or unavailable ({tried_text})",
                    "",
                    "",
                    "",
                    RowInfo(kind="error"),
                )
                self.notify(f"Tried profiles: {tried_text}. {exc}", severity="error")
                return

        if self._bucket_access_for_name(info.bucket) == BUCKET_ACCESS_NO_VIEW:
            access = BUCKET_ACCESS_NO_DOWNLOAD
            bucket_access = getattr(self.service, "bucket_access", None)
            if callable(bucket_access):
                try:
                    access = await self._call_with_sso_retry(
                        info.profile,
                        bucket_access,
                        info.profile,
                        info.bucket,
                    )
                except Exception:
                    access = BUCKET_ACCESS_NO_DOWNLOAD
            if access == BUCKET_ACCESS_NO_VIEW:
                access = BUCKET_ACCESS_NO_DOWNLOAD
            self._switch_bucket_profile(
                info.bucket,
                info.profile,
                info.profile,
                node,
                new_access=access,
            )

        if not has_any and info.prefix:
            if self._pending_target_node is node and self._pending_created:
                prev_node = self._pending_prev_node
                self._remove_pending_nodes()
                self._clear_pending()
                if prev_node is not None:
                    self.s3_tree.select_node(prev_node)
                else:
                    self.s3_tree.select_node(self.s3_tree.root)
                self.notify("Path not found", severity="warning")
                return
            self.notify("Path not found", severity="warning")
            return
        if token != self._content_token:
            if self._pending_target_node is node:
                self._clear_pending()
            return
        self._clear_table()
        self._sync_prefix_children(node, info, prefixes)
        prefixes_sorted = sorted(prefixes)
        objects_sorted = sorted(objects, key=lambda o: o.key.lower())
        rows: list[tuple[str, str, str, str, RowInfo]] = []
        for prefix in prefixes_sorted:
            name = display_segment(prefix, info.prefix)
            row_info = RowInfo(
                kind="prefix",
                profile=info.profile,
                bucket=info.bucket,
                prefix=prefix,
            )
            rows.append(
                (
                    name,
                    kind_for_row(row_info),
                    "",
                    "",
                    row_info,
                )
            )
        for obj in objects_sorted:
            name = display_segment(obj.key, info.prefix)
            row_info = RowInfo(
                kind="object",
                profile=info.profile,
                bucket=info.bucket,
                key=obj.key,
                size=obj.size,
                last_modified=obj.last_modified,
            )
            rows.append(
                (
                    name,
                    kind_for_row(row_info),
                    format_size(obj.size),
                    format_time(obj.last_modified),
                    row_info,
                )
            )
        self._content_rows = rows
        self._apply_filter(self._derive_filter(self._filter_input_value), force=True)
        if self._pending_target_node is node:
            self._clear_pending()
        stats_info = RowInfo(
            kind="bucket" if not info.prefix else "prefix",
            profile=info.profile,
            bucket=info.bucket,
            prefix=info.prefix or None,
        )
        shallow = self._collect_prefix_stats(prefixes, objects)
        self._render_prefix_stats(stats_info, shallow)
        if not suppress_history:
            self._record_history(info)

    def show_bucket_list(self) -> None:
        self._set_profile_indicator(None)
        self._clear_table()
        self._clear_selection()
        self._filter_input_value = ""
        self._preview_token += 1
        suppress_history = self._consume_history_suppression()
        rows: list[tuple[str, str, str, str, RowInfo]] = []
        for bucket in self.buckets:
            rows.append(
                (
                    bucket.name,
                    "dir",
                    "",
                    "",
                    RowInfo(kind="bucket", profile=bucket.profile, bucket=bucket.name),
                )
            )
        self._content_rows = rows
        self._apply_filter(self._derive_filter(self._filter_input_value), force=True)
        self._reset_preview()
        if not suppress_history:
            self._record_history(None)

    async def open_selected_row(self) -> None:
        row_key = self._row_key_for_cursor()
        if row_key is None:
            return
        info = self._row_info.get(row_key)
        if not info:
            return
        if info.kind == "parent":
            await self.action_up()
            return
        if info.kind == "bucket":
            if info.bucket is None:
                return
            self.navigate_to(info.profile, info.bucket, "")
            return
        if info.kind == "prefix":
            if info.bucket is None or info.prefix is None:
                return
            self.navigate_to(info.profile, info.bucket, info.prefix)
            return
        if info.kind == "object":
            return

    async def preview_selected_row(self) -> None:
        if len(self._selected_objects) >= 2:
            self._update_selection_summary()
            return
        row_key = self._row_key_for_cursor()
        if row_key is None:
            self._set_preview_header("")
            self._set_preview_text("Select a file or folder to preview.")
            return
        info = self._row_info.get(row_key)
        if not info:
            self._set_preview_header("")
            self._set_preview_text("Select a file or folder to preview.")
            return
        if info.kind in {"bucket", "prefix"} and info.bucket:
            await self._load_prefix_stats(info)
            return
        if info.kind != "object" or not info.key or not info.bucket:
            self._set_preview_header("")
            self._set_preview_text("Select a file or folder to preview.")
            return
        await self._load_preview(info)

    async def _load_prefix_stats(self, info: RowInfo) -> None:
        if not info.bucket:
            self._set_preview_header("")
            self._set_preview_text("Select a file or folder to preview.")
            return
        self._preview_token += 1
        token = self._preview_token
        self._preview_key = None
        self._preview_content = ""
        self._preview_next_start = 0
        self._preview_total = None
        self._preview_truncated = False
        self._clear_stats_state()
        self._set_preview_button("Scan", visible=True, disabled=True)
        self.preview_status.update("")
        header = self._path_for_row(info) or ""
        self._set_preview_header(header)
        self._set_preview_text("Loading stats...")
        prefix = info.prefix or ""
        try:
            prefixes, objects, _ = await self._call_with_sso_retry(
                info.profile,
                self.service.list_prefixes_and_objects,
                info.profile,
                info.bucket,
                prefix,
            )
        except Exception as exc:
            if token != self._preview_token:
                return
            self._set_preview_header(header)
            self._set_preview_text(f"Error: {exc}")
            return
        if token != self._preview_token:
            return
        shallow = self._collect_prefix_stats(prefixes, objects)
        self._render_prefix_stats(info, shallow)

    async def _load_deep_prefix_stats(self) -> None:
        info = self._preview_stats_info
        shallow = self._preview_stats_shallow
        if not info or not info.bucket or not shallow:
            return
        self._preview_token += 1
        token = self._preview_token
        header = self._path_for_row(info) or ""
        self._set_preview_header(header)
        self._set_preview_text("Scanning recursive stats...")
        self._set_preview_button("Scan", visible=True, disabled=True)
        prefix = info.prefix or ""
        try:
            deep_values = await self._call_with_sso_retry(
                info.profile,
                self.service.scan_prefix_recursive,
                info.profile,
                info.bucket,
                prefix,
                max_keys=DEEP_SCAN_MAX_KEYS,
            )
        except Exception as exc:
            if token != self._preview_token:
                return
            self._set_preview_header(header)
            self._set_preview_text(f"Error: {exc}")
            self._set_preview_button("Scan", visible=True, disabled=False)
            return
        if token != self._preview_token:
            return
        deep = DeepStats(
            files=deep_values[0],
            subdirs=deep_values[1],
            total_size=deep_values[2],
            latest_modified=deep_values[3],
            scanned=deep_values[4],
            truncated=deep_values[5],
        )
        self._render_prefix_stats(info, shallow, deep)

    async def _load_preview(self, info: RowInfo) -> None:
        self._preview_token += 1
        token = self._preview_token
        self._set_preview_header("")
        self._set_preview_text("Loading preview...")
        self._clear_stats_state()
        self._set_preview_button("More", visible=False)
        try:
            data, total, truncated = await self._call_with_sso_retry(
                info.profile,
                self.service.get_object_head,
                info.profile,
                info.bucket,
                info.key,
                max_bytes=self._preview_bytes,
            )
        except Exception as exc:
            if token != self._preview_token:
                return
            self._set_preview_header("")
            self._set_preview_text(f"Error: {exc}")
            return
        if token != self._preview_token:
            return
        self._preview_key = info
        self._preview_content = data.decode("utf-8", errors="replace")
        self._preview_next_start = len(data)
        self._preview_total = total
        self._preview_truncated = truncated
        self._render_preview()

    async def _load_more_preview(self) -> None:
        if not self._preview_key:
            return
        info = self._preview_key
        self._preview_token += 1
        token = self._preview_token
        try:
            data, total, truncated = await self._call_with_sso_retry(
                info.profile,
                self.service.get_object_range,
                info.profile,
                info.bucket,
                info.key,
                start=self._preview_next_start,
                max_bytes=self._preview_bytes,
            )
        except Exception as exc:
            if token != self._preview_token:
                return
            self._set_preview_header("")
            self._set_preview_text(f"Error: {exc}")
            return
        if token != self._preview_token:
            return
        self._preview_content += data.decode("utf-8", errors="replace")
        self._preview_next_start += len(data)
        if total is not None:
            self._preview_total = total
        self._preview_truncated = truncated
        self._render_preview()

    def navigate_to(self, profile: Optional[str], bucket: str, prefix: str) -> None:
        prev_node = self.s3_tree.cursor_node
        node, created = self.ensure_tree_path(
            profile, bucket, prefix, track_created=True
        )
        self._pending_created = created
        self._pending_prev_node = prev_node
        self._pending_target_node = node
        parent = node.parent
        while parent:
            parent.expand()
            parent = parent.parent
        self.s3_tree.select_node(node)
        self.s3_tree.scroll_to_node(node)

    def ensure_tree_path(
        self,
        profile: Optional[str],
        bucket: str,
        prefix: str,
        track_created: bool = False,
    ):
        created: list[tuple[object, str, tuple]] = []
        bucket_node = self.bucket_nodes.get((profile, bucket))
        if not bucket_node:
            access = self._bucket_access_for_name(bucket)
            bucket_node = self.s3_tree.root.add(
                self._bucket_label(
                    BucketInfo(name=bucket, profile=profile, access=access)
                ),
                data=NodeInfo(profile=profile, bucket=bucket, prefix=""),
                allow_expand=True,
            )
            self.bucket_nodes[(profile, bucket)] = bucket_node
            if track_created:
                created.append((bucket_node, "bucket", (profile, bucket)))
        current = bucket_node
        if not prefix:
            return current, created
        parent_prefix = ""
        parts = [part for part in prefix.strip("/").split("/") if part]
        for part in parts:
            parent_prefix = f"{parent_prefix}{part}/"
            key = (profile, bucket, parent_prefix)
            child = self.prefix_nodes.get(key)
            if not child:
                child = current.add(
                    part,
                    data=NodeInfo(profile=profile, bucket=bucket, prefix=parent_prefix),
                    allow_expand=True,
                )
                self.prefix_nodes[key] = child
                if track_created:
                    created.append((child, "prefix", key))
            current.expand()
            current = child
        return current, created

    def _sync_prefix_children(self, node, info: NodeInfo, prefixes: list[str]) -> None:
        if not prefixes:
            node.allow_expand = False
            return
        node.allow_expand = True
        existing = {
            child.data.prefix
            for child in node.children
            if getattr(child, "data", None) is not None
        }
        for prefix in prefixes:
            if prefix in existing:
                continue
            name = display_segment(prefix, info.prefix)
            child = node.add(
                name,
                data=NodeInfo(profile=info.profile, bucket=info.bucket, prefix=prefix),
                allow_expand=True,
            )
            self.prefix_nodes[(info.profile, info.bucket, prefix)] = child

    def _parent_prefix(self, prefix: str) -> str:
        trimmed = prefix.rstrip("/")
        if "/" not in trimmed:
            return ""
        return trimmed.rsplit("/", 1)[0] + "/"

    def _clear_table(self) -> None:
        self.s3_table.clear()
        self._row_keys = []
        self._row_info = {}

    def _add_row(
        self, name: str, kind: str, size: str, modified: str, info: RowInfo
    ) -> None:
        name_style = ""
        if info.kind == "bucket":
            name_style = self._bucket_name_style(self._bucket_access_for_name(info.bucket))
        row_key = self.s3_table.add_row(
            ellipsis_text(row_icon(info)),
            ellipsis_text(name, style=name_style),
            ellipsis_text(kind),
            size_cell(size, info.size),
            modified_cell(modified, info.last_modified),
        )
        self._row_keys.append(row_key)
        self._row_info[row_key] = info

    def _row_key_for_cursor(self):
        row = self.s3_table.cursor_row
        if row is None:
            return None
        if row < 0 or row >= len(self._row_keys):
            return None
        return self._row_keys[row]

    def _row_info_for_cursor(self) -> Optional[RowInfo]:
        row_key = self._row_key_for_cursor()
        if row_key is None:
            return None
        return self._row_info.get(row_key)

    def _restore_cursor_info(self, target: RowInfo) -> None:
        for index, row_key in enumerate(self._row_keys):
            info = self._row_info.get(row_key)
            if info == target:
                self.s3_table.move_cursor(
                    row=index,
                    column=self.s3_table.cursor_column,
                    animate=False,
                )
                return

    def _set_path_value(
        self, value: str, canonical: Optional[str] = None, suppress_filter: bool = False
    ) -> None:
        if canonical is not None:
            self._canonical_path = canonical
        display = self._strip_scheme(value)
        if not self.path_input.has_focus and self.path_input.value != display:
            if suppress_filter:
                self._suppress_filter = True
            self.path_input.value = display

    def _clear_selection(self) -> None:
        self._selected_objects.clear()
        self._selection_anchor = None
        if self._showing_selection_summary:
            self._preview_key = None
            self._preview_content = ""
            self._preview_next_start = 0
            self._preview_total = None
            self._preview_truncated = False
            self._clear_stats_state()
            self._set_preview_header("")
            self._set_preview_text("")
            self.preview_status.update("")
            self._set_preview_button("More", visible=False)
        self._showing_selection_summary = False

    def _sync_nav_buttons(self) -> None:
        if not hasattr(self, "nav_back"):
            return
        self.nav_back.disabled = self._history_index <= 0
        self.nav_forward.disabled = self._history_index >= len(self._history) - 1

    def _record_history(self, context: Optional[NodeInfo]) -> None:
        if self._history and self._history_index >= 0:
            current = self._history[self._history_index]
            if self._history_key(current) == self._history_key(context):
                self._sync_nav_buttons()
                return
        if self._history_index < len(self._history) - 1:
            self._history = self._history[: self._history_index + 1]
        self._history.append(context)
        self._history_index = len(self._history) - 1
        self._sync_nav_buttons()

    def _history_key(self, context: Optional[NodeInfo]) -> Optional[tuple]:
        if context is None:
            return None
        return (context.profile, context.bucket, context.prefix)

    def _navigate_history(self, context: Optional[NodeInfo]) -> None:
        if context is None:
            self.s3_tree.select_node(self.s3_tree.root)
            return
        self.navigate_to(context.profile, context.bucket, context.prefix)

    def _consume_history_suppression(self) -> bool:
        if self._suppress_history_once:
            self._suppress_history_once = False
            return True
        return False

    def _path_for_row(self, info: RowInfo) -> Optional[str]:
        if info.kind == "bucket" and info.bucket:
            return f"s3://{info.bucket}/"
        if info.kind == "prefix" and info.bucket and info.prefix is not None:
            prefix = info.prefix if info.prefix.endswith("/") else f"{info.prefix}/"
            return f"s3://{info.bucket}/{prefix}"
        if info.kind == "object" and info.bucket and info.key:
            if self.current_context and self.current_context.bucket == info.bucket:
                prefix = self.current_context.prefix
                if prefix:
                    return f"s3://{info.bucket}/{prefix}"
                return f"s3://{info.bucket}/"
            if "/" in info.key:
                prefix = info.key.rsplit("/", 1)[0] + "/"
                return f"s3://{info.bucket}/{prefix}"
            return f"s3://{info.bucket}/"
        return None

    def _resolve_download_path(self, target: str, info: RowInfo) -> str:
        path = Path(target).expanduser()
        if target.endswith(("/", "\\")) or (path.exists() and path.is_dir()):
            filename = Path(info.key or "download").name or "download"
            path = path / filename
        return str(path)

    def _resolve_download_dir(self, target: str) -> Path:
        path = Path(target).expanduser()
        if target.endswith(("/", "\\")):
            return path
        if path.exists():
            if path.is_dir():
                return path
            return path.parent
        return path

    def _prefix_download_name(self, info: RowInfo) -> str:
        if info.prefix:
            parts = [part for part in info.prefix.strip("/").split("/") if part]
            if parts:
                return parts[-1]
        if info.bucket:
            return info.bucket
        return "download"

    def _resolve_prefix_download_dir(self, target: str, info: RowInfo) -> Path:
        path = Path(target).expanduser()
        if target.endswith(("/", "\\")) or (path.exists() and path.is_dir()):
            return path / self._prefix_download_name(info)
        return path

    def _object_key(self, info: RowInfo) -> Optional[tuple[Optional[str], str, str]]:
        if info.kind != "object" or not info.bucket or not info.key:
            return None
        return (info.profile, info.bucket, info.key)

    def _object_path(self, info: RowInfo) -> Optional[str]:
        if info.kind != "object" or not info.bucket or not info.key:
            return None
        return f"s3://{info.bucket}/{info.key}"

    def _is_selected(self, info: RowInfo) -> bool:
        key = self._object_key(info)
        if key is None:
            return False
        return key in self._selected_objects

    def _selected_object_infos(self) -> list[RowInfo]:
        selected: list[RowInfo] = []
        for info in self._row_info.values():
            if info.kind != "object":
                continue
            key = self._object_key(info)
            if key and key in self._selected_objects:
                selected.append(info)
        return selected

    def _download_info_lines(self, selected: list[RowInfo]) -> list[str]:
        count = len(selected)
        total_size = sum(info.size or 0 for info in selected)
        paths = [self._object_path(info) for info in selected]
        paths = [path for path in paths if path]
        if not paths:
            return [f"Files: {count}", f"Total size: {format_size(total_size)}"]
        if count == 1:
            return [
                "Selected file:",
                f"  {paths[0]}",
                f"Size: {format_size(total_size)}",
            ]
        lines = [
            f"Selected files: {count}",
            f"Total size: {format_size(total_size)}",
            "Paths (first 3):",
        ]
        preview_count = min(3, len(paths))
        for path in paths[:preview_count]:
            lines.append(f"  {path}")
        if len(paths) > preview_count:
            lines.append(f"  ... and {len(paths) - preview_count} more")
        return lines

    def _download_prefix_info_lines(self, info: RowInfo) -> list[str]:
        path = self._path_for_row(info)
        if not path:
            return ["Selected folder"]
        return ["Selected folder:", f"  {path}", "Includes all files under this folder"]

    def _update_selection_summary(self) -> None:
        selected = self._selected_object_infos()
        if len(selected) >= 2:
            total_size = sum(info.size or 0 for info in selected)
            header = f"{len(selected)} files selected ({format_size(total_size)})"
            self._preview_key = None
            self._preview_content = ""
            self._preview_next_start = 0
            self._preview_total = None
            self._preview_truncated = False
            self._clear_stats_state()
            self._set_preview_header(header)
            self._set_preview_text("")
            self.preview_status.update("")
            self._set_preview_button("More", visible=False)
            self._showing_selection_summary = True
            return
        if self._showing_selection_summary:
            self._preview_key = None
            self._preview_content = ""
            self._preview_next_start = 0
            self._preview_total = None
            self._preview_truncated = False
            self._clear_stats_state()
            self._set_preview_header("")
            self._set_preview_text("")
            self.preview_status.update("")
            self._set_preview_button("More", visible=False)
            self._showing_selection_summary = False

    def handle_table_selection_click(
        self, row_index: int, shift: bool, toggle: bool
    ) -> None:
        if row_index < 0 or row_index >= len(self._row_keys):
            return
        row_key = self._row_keys[row_index]
        info = self._row_info.get(row_key)
        if not info:
            return
        if info.kind != "object":
            if not shift and not toggle:
                self._clear_selection()
                self.s3_table.refresh()
            return
        key = self._object_key(info)
        if key is None:
            return
        if shift:
            if self._selection_anchor is None:
                self._selection_anchor = row_index
            start = min(self._selection_anchor, row_index)
            end = max(self._selection_anchor, row_index)
            self._selected_objects.clear()
            for index in range(start, end + 1):
                if index < 0 or index >= len(self._row_keys):
                    continue
                candidate = self._row_info.get(self._row_keys[index])
                if not candidate or candidate.kind != "object":
                    continue
                cand_key = self._object_key(candidate)
                if cand_key:
                    self._selected_objects.add(cand_key)
        elif toggle:
            if key in self._selected_objects:
                self._selected_objects.remove(key)
            else:
                self._selected_objects.add(key)
            self._selection_anchor = row_index
        else:
            self._selected_objects = {key}
            self._selection_anchor = row_index
        self.s3_table.refresh()
        self._update_selection_summary()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "path-input":
            return
        target = self._resolve_input_path(event.value).strip()
        if not target or target == "s3://":
            self.s3_tree.select_node(self.s3_tree.root)
            return
        bucket, prefix = self._parse_s3_path(target)
        if not bucket:
            self.notify(
                "Path must include a bucket (s3://bucket/prefix/)", severity="warning"
            )
            return
        profile = self._profile_for_bucket(bucket)
        self.navigate_to(profile, bucket, prefix)

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "path-input":
            return
        if self._suppress_filter:
            self._suppress_filter = False
            return
        self._filter_input_value = event.value
        self._clear_selection()
        self._apply_filter(self._derive_filter(self._filter_input_value))

    def on_input_blurred(self, event: Input.Blurred) -> None:
        if event.input.id != "path-input":
            return
        self._set_path_value(
            self._canonical_path, canonical=self._canonical_path, suppress_filter=True
        )

    def on_key(self, event: events.Key) -> None:
        if event.key != "escape" and self._quit_escape_deadline:
            self._quit_escape_deadline = 0.0
        if self.path_input.has_focus:
            return
        if not event.is_printable or not event.character:
            return
        if event.key == "space" or event.character == " ":
            return
        if event.key in {"q", "r", "m"}:
            return
        self.set_focus(self.path_input)
        self.path_input.insert_text_at_cursor(event.character)
        event.stop()

    def _parse_s3_path(self, value: str) -> tuple[str, str]:
        path = value.strip()
        if path.startswith("s3://"):
            path = path[5:]
        path = path.lstrip("/")
        if not path:
            return "", ""
        if "/" not in path:
            return path, ""
        bucket, rest = path.split("/", 1)
        rest = rest.lstrip("/")
        if not rest:
            return bucket, ""
        if not rest.endswith("/"):
            if "/" in rest:
                rest = rest.rsplit("/", 1)[0] + "/"
            else:
                rest = ""
        return bucket, rest

    def _parse_s3_path_prefix(self, value: str) -> tuple[str, str]:
        path = value.strip()
        if path.startswith("s3://"):
            path = path[5:]
        path = path.lstrip("/")
        if not path:
            return "", ""
        if "/" not in path:
            return path, ""
        bucket, rest = path.split("/", 1)
        return bucket, rest

    def _resolve_input_path(self, value: str) -> str:
        raw = value.strip()
        if raw.startswith("s3://"):
            return raw
        raw = raw.lstrip("/")
        if not raw:
            return "s3://"
        return f"s3://{raw}"

    def _strip_scheme(self, value: str) -> str:
        if value.startswith("s3://"):
            return value[5:]
        return value

    def _derive_filter(self, value: str) -> str:
        if not self._content_rows:
            return ""
        canonical = self._canonical_path or "s3://"
        source_value = value
        if not source_value.startswith("s3://"):
            source_value = f"s3://{source_value.lstrip('/')}"
        if canonical == "s3://":
            rest = source_value
            if rest.startswith("s3://"):
                rest = rest[5:]
            rest = rest.strip()
            if not rest:
                return ""
            return rest.split("/", 1)[0]
        bucket, typed_prefix = self._parse_s3_path_prefix(source_value)
        if not self.current_context or not bucket:
            return ""
        if bucket != self.current_context.bucket:
            return ""
        current_prefix = self.current_context.prefix
        if not typed_prefix.startswith(current_prefix):
            return ""
        remainder = typed_prefix[len(current_prefix) :]
        remainder = remainder.lstrip("/")
        if "/" in remainder:
            remainder = remainder.split("/")[-1]
        return remainder

    def _sorted_content_rows(self) -> list[tuple[str, str, str, str, RowInfo]]:
        rows = list(self._content_rows)
        if not self._sort_column:
            return rows

        def is_object(row: tuple[str, str, str, str, RowInfo]) -> bool:
            return row[4].kind == "object"

        dirs = [row for row in rows if not is_object(row)]
        files = [row for row in rows if is_object(row)]

        def name_key(row: tuple[str, str, str, str, RowInfo]) -> str:
            return row[0].casefold()

        if self._sort_column == "name":
            dirs_sorted = sorted(dirs, key=name_key, reverse=self._sort_reverse)
            files_sorted = sorted(files, key=name_key, reverse=self._sort_reverse)
            return dirs_sorted + files_sorted

        if self._sort_column == "kind":
            dirs_sorted = sorted(dirs, key=name_key, reverse=self._sort_reverse)

            def kind_key(row: tuple[str, str, str, str, RowInfo]) -> tuple[str, str]:
                return (row[1].casefold(), name_key(row))

            files_sorted = sorted(files, key=kind_key, reverse=self._sort_reverse)
            return dirs_sorted + files_sorted

        if self._sort_column == "size":
            dirs_sorted = sorted(dirs, key=name_key, reverse=self._sort_reverse)

            def size_key(row: tuple[str, str, str, str, RowInfo]) -> tuple[int, str]:
                info = row[4]
                return (info.size or 0, name_key(row))

            files_sorted = sorted(files, key=size_key, reverse=self._sort_reverse)
            return dirs_sorted + files_sorted

        if self._sort_column == "modified":
            dirs_sorted = sorted(dirs, key=name_key, reverse=self._sort_reverse)

            def modified_key(
                row: tuple[str, str, str, str, RowInfo],
            ) -> tuple[datetime, str]:
                info = row[4]
                return (info.last_modified or datetime.min, name_key(row))

            files_sorted = sorted(files, key=modified_key, reverse=self._sort_reverse)
            return dirs_sorted + files_sorted

        return rows

    def _apply_filter(self, text: str, force: bool = False) -> None:
        if not force and text == self._active_filter:
            return
        self._active_filter = text
        self._clear_table()
        for name, kind, size, modified, info in self._sorted_content_rows():
            if info.kind == "parent":
                self._add_row(name, kind, size, modified, info)
                continue
            if not text or name.startswith(text):
                self._add_row(name, kind, size, modified, info)
        self.s3_table.call_after_refresh(self._resize_table_columns)

    def _profile_for_bucket(self, bucket: str) -> Optional[str]:
        for info in self.buckets:
            if info.name == bucket:
                return info.profile
        for (profile, name), _node in self.bucket_nodes.items():
            if name == bucket:
                return profile
        return None

    def _set_preview_text(self, text: str) -> None:
        if not text:
            self.preview.load_text("")
            return
        self.preview.load_text(text)

    def _set_preview_header(self, text: str) -> None:
        self.preview_header.update(text)

    def _set_preview_button(
        self, label: str, visible: bool, disabled: bool = False
    ) -> None:
        self.preview_more.label = label
        self.preview_more.disabled = disabled
        if visible:
            self.preview_more.remove_class("hidden")
        else:
            self.preview_more.add_class("hidden")

    def _clear_stats_state(self) -> None:
        self._preview_stats_info = None
        self._preview_stats_shallow = None
        self._preview_stats_deep = None

    def _collect_prefix_stats(
        self, prefixes: list[str], objects: list[ObjectInfo]
    ) -> PrefixStats:
        dir_count = len(prefixes)
        file_count = len(objects)
        total_size = sum(obj.size for obj in objects)
        latest_modified = max(
            (obj.last_modified for obj in objects if obj.last_modified),
            default=None,
        )
        return PrefixStats(
            dirs=dir_count,
            files=file_count,
            total_size=total_size,
            latest_modified=latest_modified,
        )

    def _render_prefix_stats(
        self,
        info: RowInfo,
        shallow: PrefixStats,
        deep: Optional[DeepStats] = None,
    ) -> None:
        header = self._path_for_row(info) or ""
        lines = [
            f"Folders: {shallow.dirs}",
            f"Files: {shallow.files}",
            f"Total size: {format_size(shallow.total_size)}",
        ]
        if shallow.latest_modified:
            lines.append(f"Latest modified: {format_time(shallow.latest_modified)}")
        if deep is None:
            lines.append("Total files (recursive): press 'm' to scan")
            lines.append("Total subdirs (recursive): press 'm' to scan")
            lines.append("Total size (recursive): press 'm' to scan")
            lines.append("Scope: immediate children")
            self._set_preview_button("Scan", visible=True)
        else:
            files_line = f"Total files (recursive): {deep.files}"
            subdirs_line = f"Total subdirs (recursive): {deep.subdirs}"
            size_line = f"Total size (recursive): {format_size(deep.total_size)}"
            if deep.truncated:
                files_line = (
                    f"Total files (recursive): >= {deep.files} "
                    f"(scanned {deep.scanned} objects)"
                )
                subdirs_line = f"Total subdirs (recursive): >= {deep.subdirs} (partial)"
                size_line = f"Total size (recursive): >= {format_size(deep.total_size)} (partial)"
            lines.extend([files_line, subdirs_line, size_line])
            lines.append("Scope: immediate children + recursive totals")
            self._set_preview_button("Scan", visible=False)
        self._set_preview_header(header)
        self._set_preview_text("\n".join(lines))
        self.preview_status.update(f"{shallow.dirs} dirs, {shallow.files} files")
        self._preview_key = None
        self._preview_content = ""
        self._preview_next_start = 0
        self._preview_total = None
        self._preview_truncated = False
        self._preview_stats_info = info
        self._preview_stats_shallow = shallow
        self._preview_stats_deep = deep

    def _update_sort_headers(self) -> None:
        if (
            not self._col_name
            or not self._col_kind
            or not self._col_size
            or not self._col_modified
        ):
            return
        if not hasattr(self, "s3_table"):
            return
        column_map = {
            "name": self._col_name,
            "kind": self._col_kind,
            "size": self._col_size,
            "modified": self._col_modified,
        }
        base_labels = {
            self._col_name: "Name",
            self._col_kind: "Kind",
            self._col_size: "Size",
            self._col_modified: "Modified",
        }
        arrow = "▲" if not self._sort_reverse else "▼"
        sorted_key = column_map.get(self._sort_column)
        for key, base in base_labels.items():
            label_text = base
            if sorted_key is not None and key == sorted_key:
                label_text = f"{base} {arrow}"
            self.s3_table.columns[key].label = Text(label_text)
        self.s3_table.refresh()
        self.s3_table.call_after_refresh(self._resize_table_columns)

    def _render_preview(self) -> None:
        if not self._preview_key:
            self._set_preview_header("")
            self._set_preview_text("")
            return
        self._clear_stats_state()
        loaded = self._preview_next_start
        if self._preview_total is not None:
            header = (
                f"{self._preview_key.key} "
                f"({format_size(loaded)} of {format_size(self._preview_total)})"
            )
        else:
            header = f"{self._preview_key.key} (first {format_size(loaded)})"
        footer = ""
        if self._preview_truncated:
            if self._preview_total:
                percent = int((loaded / self._preview_total) * 100)
                footer = (
                    "\n\n=== MORE AVAILABLE ===\n"
                    f"Loaded ~{format_size(loaded)} ({percent}%). "
                    "Press 'm' or click More"
                )
            else:
                footer = (
                    "\n\n=== MORE AVAILABLE ===\n"
                    f"Loaded ~{format_size(loaded)}. "
                    "Press 'm' or click More"
                )
            self._set_preview_button("More", visible=True)
        else:
            self._set_preview_button("More", visible=False)
        self._set_preview_header(header)
        self._set_preview_text(f"{self._preview_content}{footer}")
        if self._preview_total:
            self.preview_status.update(
                f"{format_size(loaded)}/{format_size(self._preview_total)}"
            )
        else:
            self.preview_status.update(f"{format_size(loaded)}")

    def _reset_preview(self) -> None:
        self._preview_key = None
        self._preview_content = ""
        self._preview_next_start = 0
        self._preview_total = None
        self._preview_truncated = False
        self._clear_stats_state()
        self._set_preview_header("")
        self._set_preview_text("")
        self.preview_status.update("")
        self._set_preview_button("More", visible=False)

    def _remove_pending_nodes(self) -> None:
        for node, kind, key in reversed(self._pending_created):
            if kind == "prefix":
                self.prefix_nodes.pop(key, None)
            elif kind == "bucket":
                self.bucket_nodes.pop(key, None)
            try:
                node.remove()
            except Exception:
                pass

    def _clear_pending(self) -> None:
        self._pending_created = []
        self._pending_prev_node = None
        self._pending_target_node = None


def _parse_profiles(args: argparse.Namespace) -> Optional[list[str]]:
    profiles: list[str] = []
    if args.profiles:
        for part in args.profiles.split(","):
            value = part.strip()
            if value:
                profiles.append(value)
    if args.profile:
        profiles.extend(args.profile)
    return profiles or None


def main() -> None:
    parser = argparse.ArgumentParser(description="Textual S3 browser")
    parser.add_argument(
        "--profiles",
        help="Comma-separated AWS profiles to load (defaults to all available)",
    )
    parser.add_argument(
        "-p",
        "--profile",
        action="append",
        help="AWS profile to add (can be used multiple times)",
    )
    parser.add_argument(
        "--region",
        help="AWS region override for S3 client",
    )
    args = parser.parse_args()
    profiles = _parse_profiles(args)
    app = S3Browser(profiles=profiles, region=args.region)
    app.run()


if __name__ == "__main__":
    main()
