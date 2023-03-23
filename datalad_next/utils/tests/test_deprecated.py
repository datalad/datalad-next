from datalad_next.utils.deprecate import deprecated

import pytest


@deprecated(msg='nothing to see here', version='1.0')
def deprecated_function(inputstring):
    return inputstring


class RandomClass(object):

    @deprecated(msg="nothing to see here", version='1.0')
    def deprecated_method(self, inputstring):
        return inputstring


@deprecated(msg="nothing to see here", version='1.0')
class DeprecatedClass(object):

    def __call__(self, inputstring):
        return inputstring


@deprecated(msg='nothing to see here', kwarg='inputmode', version='1.0')
def deprecated_function_param(inputmode='default', other_param=None):
    return inputmode


class RandomClassParam(object):

    @deprecated(msg="nothing to see here", kwarg='inputmode', version='1.0')
    def deprecated_method(self, inputmode='default', other_param=None):
        return inputmode


@deprecated(msg='nothing to see here', kwarg='inputmode',
            kwarg_values=['default'], version='1.0')
def deprecated_function_param_value(inputmode='default'):
    return inputmode


@deprecated(msg='nothing to see here', kwarg='inputmode',
            kwarg_values=[None], version='1.0')
def deprecated_function_param_value_none(inputmode=None):
    return inputmode


class RandomClassParamValue(object):

    @deprecated(msg="nothing to see here", kwarg='inputmode',
                kwarg_values=['default'], version='1.0')
    def deprecated_method(self, inputmode='default'):
        return inputmode


@deprecated(msg='nothing to see here', version='1.0', kwarg='mode')
@deprecated(msg='even less to see here', version='1.0', kwarg='othermode')
def double_deprecated_function(mode='default', othermode='moredefault'):
    return (mode, othermode)


@deprecated(msg='nothing to see here', version='1.0', kwarg='mode',
            kwarg_values=['1', '2'])
def two_deprecated_values(mode='default'):
    return mode


def test_deprecated():
    # deprecations for entire functions/classes
    input_string = 'hello world'
    for func in [deprecated_function,
                 RandomClass().deprecated_method]:
        with pytest.warns(
                DeprecationWarning,
                match=f"{func.__module__}.{func.__name__} was deprecated in version 1.0. nothing to see here"):
            res = func(inputstring=input_string)
            assert res == input_string

    with pytest.warns(DeprecationWarning, match="nothing to see here"):
        DeprecatedClass()

    # deprecations for a kwarg
    inputmode = 'default'
    for func in [deprecated_function_param,
                 RandomClassParam().deprecated_method]:
        with pytest.warns(DeprecationWarning, match="argument 'inputmode'"):
            res = func(inputmode=inputmode)
            assert res == inputmode

    # deprecations for a kwarg value
    for func in [deprecated_function_param_value,
                 RandomClassParamValue().deprecated_method,
                 ]:
        with pytest.warns(
                DeprecationWarning,
                match="Use of values {'default'} for argument 'inputmode'"):
            res = func(inputmode=inputmode)
            assert res == inputmode

    # `None` value deprecation is supported
    # test in many complicated forms
    for v in (None, (None,), [None], {None: 'some'}):
        with pytest.warns(
                DeprecationWarning,
                match="Use of values {None} for argument 'inputmode'"):
            res = deprecated_function_param_value_none(inputmode=v)
            assert res == v

    # no deprecations for an unused deprecated parameter or parameter value
    for func in [deprecated_function_param_value,
                 RandomClassParamValue().deprecated_method,
                 ]:
        with pytest.warns(None) as record:
            res = func(inputmode='not-deprecated')
            assert res == 'not-deprecated'
            assert len(record) == 0

        for func in [deprecated_function_param,
                     RandomClassParam().deprecated_method]:
            with pytest.warns(None) as record:
                res = func(other_param='something!')
                assert res == inputmode
                assert len(record) == 0

    # make sure it catches the parameter even if its a list
    for func in [deprecated_function_param_value,
                 RandomClassParamValue().deprecated_method,
                 ]:
        with pytest.warns(
                DeprecationWarning,
                match="Use of values {'default'} for argument 'inputmode'"):
            res = func(inputmode=[inputmode])
            assert res == [inputmode]
        with pytest.warns(None) as record:
            res = func(inputmode=['not-deprecated'])
            assert res == ['not-deprecated']
            assert len(record) == 0

    # two decorators work as expected
    with pytest.warns(DeprecationWarning) as record:
        res = double_deprecated_function(mode='1', othermode='2')
        assert res == ('1', '2')
        assert len(record) == 2
        assert 'nothing to see here' in str(record.list[0].message)
        assert 'even less to see here' in str(record.list[1].message)

    # test that everything works when the function has several deprecated values

    with pytest.warns(DeprecationWarning):
        res = two_deprecated_values(mode='1')
        assert res == '1'
        # shouldn't matter if the parameter value is a list
        res = two_deprecated_values(mode=['1'])
        assert res == ['1']
    with pytest.warns(None) as record:
        res = two_deprecated_values(mode='safe')
        assert res == 'safe'
        assert len(record) == 0
