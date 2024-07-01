"""Enable reproducible documentation

The original documentation generation tooling can include memory
addresses in the text and thereby prevent reproducible builds
with unnecessary noise.

This patch uses regex to look for and delete such information
in a similar way to what sphinx is doing anyway in other places.
"""

import re

from datalad.interface.base import (
    alter_interface_docs_for_api,
    is_api_arg,
)

from . import apply_patch

obj_address_repr = re.compile(' at 0x[0-9a-f]+')


# This function interface is initially taken from
# datalad-core@982cca549ae29a1c86a0d6736bc3d6dfec370433
def update_docstring_with_parameters(func, params, prefix=None, suffix=None,
                                     add_args=None):
    """Generate a useful docstring from a parameter spec

    Amends any existing docstring of a callable with a textual
    description of its parameters. The Parameter spec needs to match
    the number and names of the callables arguments.
    """
    from datalad.utils import getargspec

    # get the signature
    args, varargs, varkw, defaults = getargspec(func, include_kwonlyargs=True)
    defaults = defaults or tuple()
    if add_args:
        add_argnames = sorted(add_args.keys())
        args.extend(add_argnames)
        defaults = defaults + tuple(add_args[k] for k in add_argnames)
    ndefaults = len(defaults)
    # start documentation with what the callable brings with it
    doc = prefix if prefix else u''
    if len(args) > 1:
        if len(doc):
            if not doc.endswith('\n'):
                doc += '\n'
            doc += '\n'
        doc += "Parameters\n----------\n"
        for i, arg in enumerate(args):
            if not is_api_arg(arg):
                continue
            # we need a parameter spec for each argument
            if not arg in params:
                raise ValueError("function has argument '%s' not described as a parameter" % arg)
            param = params[arg]
            # validate the default -- to make sure that the parameter description is
            # somewhat OK
            defaults_idx = ndefaults - len(args) + i
            if defaults_idx >= 0:
                if param.constraints is not None:
                    param.constraints(defaults[defaults_idx])
            orig_docs = param._doc
            param._doc = alter_interface_docs_for_api(param._doc)
            autodoc = param.get_autodoc(
                arg,
                default=defaults[defaults_idx] if defaults_idx >= 0 else None,
                has_default=defaults_idx >= 0)
            # PATCH replace any memory address in a repr()-like report
            # with nothing to make doc generation reproducible
            doc += obj_address_repr.sub('', autodoc)
            param._doc = orig_docs
            doc += '\n'
    doc += suffix if suffix else u""
    # assign the amended docs
    func.__doc__ = doc
    return func


apply_patch(
    'datalad.interface.base', None, 'update_docstring_with_parameters',
    update_docstring_with_parameters,
)
