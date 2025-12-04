from __future__ import annotations
from typing import Generic, Iterable, TypeVar, Type

SelectType = TypeVar("SelectType")

class Widget:
    pass

class Button(Widget):
    id: str | None
    variant: str | None

    class Pressed:
        button: Button

    def __init__(self, label: str, id: str | None = None, variant: str | None = None) -> None: ...

class Footer(Widget): ...
class Header(Widget):
    def __init__(self, show_clock: bool = ...) -> None: ...

class Input(Widget):
    value: str
    placeholder: str

    def __init__(self, value: str = "", placeholder: str = "") -> None: ...

class TextArea(Widget):
    value: str
    text: str
    placeholder: str
    soft_wrap: bool

    def __init__(self) -> None: ...

class SelectOption:
    def __init__(self, label: str, value: object) -> None: ...


class Select(Generic[SelectType], Widget):
    value: SelectType | None
    Option: Type[SelectOption]

    def __init__(
        self,
        options: Iterable[SelectOption],
        prompt: str | None = None,
        allow_blank: bool = False,
        value: SelectType | None = None,
    ) -> None: ...

class Label(Widget):
    def __init__(self, label: str, classes: str | None = None) -> None: ...

class Static(Widget):
    def __init__(self, text: str, classes: str | None = None) -> None: ...
    def update(self, text: str) -> None: ...
