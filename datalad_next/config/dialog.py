from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    'Dialog',
    'Question',
    'YesNo',
    'Choice',
]


@dataclass(kw_only=True)
class Dialog:
    title: str
    text: str


@dataclass(kw_only=True)
class Question(Dialog):
    pass


@dataclass(kw_only=True)
class YesNo(Dialog):
    pass


@dataclass(kw_only=True)
class Choice(Dialog):
    pass
