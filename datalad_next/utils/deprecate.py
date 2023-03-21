import inspect
import warnings
from functools import wraps

from datalad_next.utils import get_wrapped_class


def deprecated(msg, version, parameter=None, parameter_choice=None):
    """Annotate functions, classes, or function parameters with standardized
    deprecation warnings.

    Parameters
    ----------
    version: str
      Software version number at which the deprecation was made
    msg: str
      Custom message to append to a deprecation warning
    parameter: str
      Individual parameter being deprecated (instead of entire function/class)
    parameter_choice: str
      Individual parameter choice of specified parameter being deprecated
    """

    def check_parameters(funcargs):
        if parameter not in funcargs:
            return False
        if parameter_choice is not None:
            val = funcargs[parameter]
            if isinstance(val, list):
                if parameter_choice not in val:
                    return False
            elif isinstance(parameter_choice, list):
                if val not in parameter_choice:
                    return False
            else:
                if val != parameter_choice:
                    return False
        return True

    base_template = "{func} was deprecated in version {version}. {msg}"

    def decorator(func):
        @wraps(func)
        def func_with_deprecation_warning(*args, **kwargs):
            fname = (
                    get_wrapped_class(func)
                    if func.__name__ == "__call__"
                    else func.__name__
                    )

            if parameter is None:
                warning = base_template
            else:
                if not check_parameters(kwargs):
                    return func(*args, **kwargs)
                if parameter_choice is not None:
                    warning = (
                            f"The parameter value {parameter_choice} of "
                            f"parameter {parameter} of " + base_template
                    )
                else:
                    warning = f"The {parameter} parameter of " + base_template
            warnings.warn(
                warning.format(func=fname, version=version, msg=msg),
                DeprecationWarning,
            )
            return func(*args, **kwargs)

        return func_with_deprecation_warning

    return decorator
