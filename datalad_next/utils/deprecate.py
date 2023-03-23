import warnings
from functools import wraps


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
                warnings.warn(
                    base_template.format(
                        func=f'{func.__module__}.{func.__name__}',
                        version=version,
                        msg=msg,
                    ),
                    DeprecationWarning,
                )
                return func(*args, **kwargs)

            return func_with_deprecation_warning

        return decorator

    # a single parameter, or parameter choice is deprecated
    def decorator(func):
        @wraps(func)
        def func_with_deprecation_warning(*args, **kwargs):
            # has a deprecated parameter been used?
            if parameter not in kwargs.keys():
                # there is nothing to deprecate
                return func(*args, **kwargs)
            # has a deprecated parameter choice been used?
            if parameter_choice is not None:
                val = kwargs[parameter]
                if isinstance(val, list):
                    if parameter_choice not in val:
                        # there is nothing to deprecate
                        return func(*args, **kwargs)
                elif isinstance(parameter_choice, list):
                    if val not in parameter_choice:
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
            warnings.warn(
                template.format(
                    parameter=parameter,
                    func=f'{func.__module__}.{func.__name__}',
                    version=version,
                    msg=msg,
                ),
                DeprecationWarning,
            )
            return func(*args, **kwargs)

        return func_with_deprecation_warning

    return decorator
