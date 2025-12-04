from __future__ import annotations

from types import TracebackType
from typing import Optional, Type


class Container:
    def __enter__(self) -> Container: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None: ...

class VerticalScroll(Container):
    def __init__(self, id: str | None = None) -> None: ...
