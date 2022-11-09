from json import loads

from .base import Constraint


class EnsureJSON(Constraint):
    """Ensures that string is JSON formated and can be deserialized.
    """
    def __init__(self):
        super().__init__()

    def __call__(self, value):
        return loads(value)

    def short_description(self):
        return 'JSON'
