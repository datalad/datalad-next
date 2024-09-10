from __future__ import annotations

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Callable,
    Generator,
)


class ResultHandler(ABC):
    @abstractmethod
    def return_results(self, get_results: Callable):
        """ """

    @abstractmethod
    def log_result(self, result: dict) -> None:
        """ """

    @abstractmethod
    def want_custom_result_summary(self, mode: str) -> bool:
        """ """

    @abstractmethod
    def render_result(self, result: dict) -> None:
        """ """

    @abstractmethod
    def render_result_summary(self) -> None:
        """ """

    @abstractmethod
    def run_result_hooks(self, res) -> Generator[dict[str, Any], None, None]:
        """ """

    @abstractmethod
    def transform_result(self, res) -> Generator[Any, None, None]:
        """ """

    @abstractmethod
    def keep_result(self, res) -> bool:
        """ """
