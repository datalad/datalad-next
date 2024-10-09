from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    'Dialog',
    'Question',
    'YesNo',
    'Choice',
]


# only from PY3.10
# @dataclass(kw_only=True)
@dataclass
class Dialog:
    title: str
    text: str | None = None


@dataclass
class Question(Dialog):
    pass


@dataclass
class YesNo(Dialog):
    pass


@dataclass
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
