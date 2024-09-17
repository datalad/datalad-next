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


def get_dialog_class_from_legacy_ui_label(label: str) -> type[Dialog]:
    """Recode legacy `datalad.interface.common_cfg` UI type label"""
    if label == 'yesno':
        return YesNo
    elif label == 'question':
        return Question
    else:
        msg = f'unknown UI type label {label!r}'
        raise ValueError(msg)
