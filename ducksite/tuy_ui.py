from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from textual.app import App, ComposeResult
from textual.containers import Container, VerticalScroll
from textual.widgets import Button, Footer, Header, Input, Label, Select, Static, TextArea


@dataclass
class FieldSpec:
    """Describe a single prompt displayed inside the Textual TUY form."""

    name: str
    label: str
    default: str = ""
    placeholder: str = ""
    optional: bool = False
    help_text: Optional[str] = None
    choices: Optional[List[tuple[str, str]]] = None
    multiline: bool = False


class _FormApp(App[Dict[str, str] | None]):
    BINDINGS = [
        ("ctrl+s", "save_form", "Save form"),
        ("ctrl+c", "cancel_form", "Cancel form"),
        ("ctrl+n", "next_field", "Next field"),
        ("ctrl+p", "prev_field", "Previous field"),
    ]

    CSS = """
    Screen {
        background: $surface;
    }

    # Accent colors to keep the TUY readable and friendly.
    .title {
        color: $accent-lighten-1;
        text-style: bold;
        padding: 1 0 0 0;
    }

    .subtitle {
        color: $accent-lighten-2;
        padding-bottom: 1;
    }

    .field-label {
        color: $text;
        text-style: bold;
        padding-top: 1;
    }

    .field-help {
        color: $text-muted;
        padding-bottom: 1;
    }

    .status {
        color: $warning-darken-1;
        padding: 1 0 0 0;
    }

    # Buttons tuned for quick navigation.
    Button {
        margin-top: 1;
    }
    """

    def __init__(
        self,
        title: str,
        instructions: str,
        fields: List[FieldSpec],
    ) -> None:
        super().__init__()
        self._title = title
        self._instructions = instructions
        self._fields = fields
        self._inputs: Dict[str, Input | TextArea] = {}
        self._selects: Dict[str, Select[str]] = {}
        self._status: Optional[Static] = None
        self._ordered_widgets: List[Input | TextArea | Select[str]] = []

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with VerticalScroll(id="body"):
            yield Static(self._title, classes="title")
            if self._instructions:
                yield Static(self._instructions, classes="subtitle")

            for field in self._fields:
                with Container():
                    yield Label(field.label, classes="field-label")
                    if field.help_text:
                        yield Static(field.help_text, classes="field-help")

                    if field.choices:
                        select = Select[str](
                            (
                                Select.Option(label, value)
                                for value, label in field.choices
                            ),
                            prompt=field.placeholder or "Select an option",
                            allow_blank=field.optional,
                            value=field.default or None,
                        )
                        self._selects[field.name] = select
                        self._ordered_widgets.append(select)
                        yield select
                    elif field.multiline:
                        text_area = TextArea()
                        text_area.value = field.default
                        text_area.placeholder = field.placeholder
                        text_area.soft_wrap = True
                        self._inputs[field.name] = text_area  # type: ignore[assignment]
                        self._ordered_widgets.append(text_area)
                        yield text_area
                    else:
                        input_widget = Input(
                            value=field.default,
                            placeholder=field.placeholder,
                        )
                        self._inputs[field.name] = input_widget
                        self._ordered_widgets.append(input_widget)
                        yield input_widget

            self._status = Static("", classes="status")
            yield self._status
            with Container():
                yield Button("Save", id="save", variant="success")
                yield Button("Cancel", id="cancel", variant="error")
        yield Footer()

    def on_mount(self) -> None:  # pragma: no cover - UI
        if self._ordered_widgets:
            self.set_focus(self._ordered_widgets[0])
        if self._status:
            self._status.update(
                "Tab or Ctrl+N/Ctrl+P move between fields. Ctrl+S saves, Ctrl+C cancels."
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:  # pragma: no cover - UI
        button_id = event.button.id
        if button_id == "cancel":
            self.exit(None)
            return
        if button_id != "save":
            return

        self._collect_and_exit()

    def action_save_form(self) -> None:  # pragma: no cover - UI
        self._collect_and_exit()

    def action_cancel_form(self) -> None:  # pragma: no cover - UI
        self.exit(None)

    def _focus_by_offset(self, delta: int) -> None:  # pragma: no cover - UI
        if not self._ordered_widgets:
            return
        current = self.focused
        try:
            idx = self._ordered_widgets.index(current) if current else 0
        except ValueError:
            idx = 0
        next_idx = (idx + delta) % len(self._ordered_widgets)
        self.set_focus(self._ordered_widgets[next_idx])

    def action_next_field(self) -> None:  # pragma: no cover - UI
        self._focus_by_offset(1)

    def action_prev_field(self) -> None:  # pragma: no cover - UI
        self._focus_by_offset(-1)

    def _collect_and_exit(self) -> None:
        
        result: Dict[str, str] = {}
        for field in self._fields:
            if field.choices:
                value = self._selects[field.name].value or ""
            else:
                widget = self._inputs[field.name]
                if isinstance(widget, TextArea):
                    value = widget.text.strip()
                else:
                    value = widget.value.strip()

            if not value and not field.optional:
                assert self._status is not None
                self._status.update(f"{field.label} is required.")
                return
            result[field.name] = value

        self.exit(result)


def prompt_form(title: str, instructions: str, fields: List[FieldSpec]) -> Dict[str, str]:
    """Render a simple, colorful Textual form and return user responses."""

    nav_help = (
        "Navigation: Tab or Ctrl+N/Ctrl+P move between fields; Ctrl+S saves;"
        " Ctrl+C cancels. Inputs are validated with the same checks as a build."
    )
    merged_instructions = instructions.strip()
    if merged_instructions:
        merged_instructions = merged_instructions + "\n\n" + nav_help
    else:
        merged_instructions = nav_help

    app = _FormApp(title, merged_instructions, fields)
    result = app.run()
    if result is None:
        raise KeyboardInterrupt("Cancelled")
    return result
