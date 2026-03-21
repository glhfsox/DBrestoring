"""Typing-friendly base for GUI mixins.

The Tk app is assembled through multiple mixins, so static analyzers do not see
the full attribute surface on each mixin class in isolation. This base provides
``__getattr__`` as an ``Any`` fallback for those composed attributes.
"""

from __future__ import annotations

from typing import Any


class GUIBoundMixin:
    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)
