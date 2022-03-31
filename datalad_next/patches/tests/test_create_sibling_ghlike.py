# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""Test create publication target on Github-like platforms"""

from datalad.distributed.tests.test_create_sibling_ghlike import *
from datalad.distributed.tests.test_create_sibling_gin import *
from datalad.distributed.tests.test_create_sibling_gitea import *
from datalad.distributed.tests.test_create_sibling_github import *
from datalad.distributed.tests.test_create_sibling_gogs import *


# we overwrite this one from core, because it assumed the old credential
# system to be used
@with_tempfile
def test_invalid_call(path):
    # no dataset
    assert_raises(ValueError, create_sibling_gin, 'bogus', dataset=path)
    ds = Dataset(path).create()
    # unsupported name
    assert_raises(
        ValueError,
        ds.create_sibling_gin, 'bo  gus', credential='some')

    # conflicting sibling name
    ds.siblings('add', name='gin', url='http://example.com',
                result_renderer='disabled')
    res = ds.create_sibling_gin(
        'bogus', name='gin', credential='some', on_failure='ignore',
        dry_run=True)
    assert_status('error', res)
    assert_in_results(
        res,
        status='error',
        message=('already has a configured sibling "%s"', 'gin'))
