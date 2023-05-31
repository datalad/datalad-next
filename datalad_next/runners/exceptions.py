# exceptions
from datalad.runner import (
    CommandError as _CommandError,
)


class CommandError(_CommandError):
    # without overwriting __repr__ it would use RuntimeError's variant
    # with ignore all info but `.msg` which will be empty frequently
    # and confuse people with `CommandError('')`
    def __repr__(self) -> str:
        return self.to_str()
