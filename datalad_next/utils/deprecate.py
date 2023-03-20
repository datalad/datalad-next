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

    base_template = "{func} was deprecated in version {version}. {msg}"

    if parameter is None:
        # the entire class/function is deprecated
        def decorator(func):
            @wraps(func)
            def func_with_deprecation_warning(*args, **kwargs):
                fname = (
                    get_wrapped_class(func)
                    if func.__name__ == "__call__"
                    else func.__name__
                )
                warnings.warn(
                    base_template.format(func=fname, version=version, msg=msg),
                    DeprecationWarning,
                )
                return func(*args, **kwargs)

            return func_with_deprecation_warning

        return decorator

    # a single parameter, or parameter choice is deprecated
    def decorator(func):
        @wraps(func)
        def func_with_deprecation_warning(*args, **kwargs):
            # obtain the signature, and check if there are reasons to warn
            args_info = inspect.getargvalues(inspect.currentframe())
            # only keyword arguments can be deprecated
            funcargs = {
                arg: args_info.locals["kwargs"].get(arg)
                for arg in args_info.locals["kwargs"]
            }
            # has a deprecated parameter been used?
            if parameter not in funcargs.keys():
                # there is nothing to deprecate
                return func(*args, **kwargs)
            # has a deprecated parameter choice been used?
            if parameter_choice is not None:
                val = funcargs[parameter]
                if isinstance(val, list):
                    if parameter_choice not in val:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                else:
                    if val != parameter_choice:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                template = (
                    f"The parameter value {parameter_choice} of "
                    f"parameter {parameter} of " + base_template
                )
            else:
                template = "The {parameter} parameter of " + base_template
            fname = (
                get_wrapped_class(func)
                if func.__name__ == "__call__"
                else func.__name__
            )
            warnings.warn(
                template.format(
                    parameter=parameter, func=fname, version=version, msg=msg
                ),
                DeprecationWarning,
            )
            return func(*args, **kwargs)

        return func_with_deprecation_warning

    return decorator
