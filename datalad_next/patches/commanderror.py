from datalad.runner.exception import CommandError


def commanderror_repr(self) -> str:
    return self.to_str()


# without overwriting __repr__ it would use RuntimeError's variant
# with ignore all info but `.msg` which will be empty frequently
# and confuse people with `CommandError('')`
CommandError.__repr__ = commanderror_repr
