import pytest
import subprocess

from .. import (
    GitPathSpec,
)


def _list_files(path, pathspecs):
    return [
        i for i in subprocess.run(
            ['git', 'ls-files', '-z', '--other', '--', *pathspecs],
            capture_output=True,
            cwd=path,
        ).stdout.decode('utf-8').split('\0')
        if i
    ]


@pytest.fixture(scope="function")
def pathspec_match_testground(tmp_path_factory):
    """Create a Git repo with no commit and many untracked files

    In this playground, `git ls-files --other` can be used to testrun
    pathspecs.

    See the top item in `testcases` for a summary of the content
    """
    p = tmp_path_factory.mktemp('pathspec_match')
    subprocess.run(['git', 'init'], cwd=p, check=True)
    p_sub = p / 'sub'
    p_sub.mkdir()
    for d in (p, p_sub):
        p_a = d / 'aba'
        p_b = d / 'a?a'
        for sp in (p_a, p_b):
            sp.mkdir()
            for fname in ('a.txt', 'A.txt', 'a.JPG'):
                (sp / fname).touch()
    # add something that is unique to sub/
    (p_sub / 'b.dat').touch()
    yield p


testcases = [
    # valid
    dict(
        ps=':',
        fordir={
            None: {'specs': [':'],
                   'match': [
                       'a?a/A.txt', 'a?a/a.JPG', 'a?a/a.txt',
                       'aba/A.txt', 'aba/a.JPG', 'aba/a.txt',
                       'sub/a?a/A.txt', 'sub/a?a/a.JPG', 'sub/a?a/a.txt',
                       'sub/aba/A.txt', 'sub/aba/a.JPG', 'sub/aba/a.txt',
                       'sub/b.dat'],
            },
            'sub': {'specs': [],
                    'match': [
                        'a?a/A.txt', 'a?a/a.JPG', 'a?a/a.txt',
                        'aba/A.txt', 'aba/a.JPG', 'aba/a.txt',
                        'b.dat'],
            },
        },
    ),
    dict(
        ps='aba',
        fordir={
            None: {'match': ['aba/A.txt', 'aba/a.JPG', 'aba/a.txt']},
            'aba': {'specs': [],
                    'match': ['A.txt', 'a.JPG', 'a.txt']},
        },
    ),
    # same as above, but with a trailing slash
    dict(
        ps='aba/',
        fordir={
            None: {'match': ['aba/A.txt', 'aba/a.JPG', 'aba/a.txt']},
            'aba': {'specs': [],
                    'match': ['A.txt', 'a.JPG', 'a.txt']},
        },
    ),
    dict(
        ps=':(glob)aba/*.txt',
        fordir={
            None: {'match': ['aba/A.txt', 'aba/a.txt']},
        },
    ),
    dict(
        ps=':/aba/*.txt',
        norm=':(top)aba/*.txt',
        fordir={
            None: {'match': ['aba/A.txt', 'aba/a.txt']},
            # for a subdir a keeps matching the exact same items
            # not only be name, but by location
            'sub': {'specs': [':(top)aba/*.txt'],
                    'match': ['../aba/A.txt', '../aba/a.txt']},
        },
    ),
    dict(
        ps='aba/*.txt',
        fordir={
            None: {'match': ['aba/A.txt', 'aba/a.txt']},
            # not applicable
            'sub': {'specs': []},
            # but this is
            'aba': {'specs': ['*.txt']},
        },
    ),
    dict(
        ps='sub/aba/*.txt',
        fordir={
            None: {'match': ['sub/aba/A.txt', 'sub/aba/a.txt']},
            'sub': {'specs': ['aba/*.txt'],
                    'match': ['aba/A.txt', 'aba/a.txt']},
        },
    ),
    dict(
        ps='*.JPG',
        fordir={
            None: {'match': ['a?a/a.JPG', 'aba/a.JPG', 'sub/a?a/a.JPG',
                             'sub/aba/a.JPG']},
            # unchanged
            'sub': {'specs': ['*.JPG']},
        },
    ),
    dict(
        ps='*ba*.JPG',
        fordir={
            None: {'match': ['aba/a.JPG', 'sub/aba/a.JPG']},
            'aba': {'specs': ['*ba*.JPG', '*.JPG'],
                    'match': ['a.JPG']},
        },
    ),
    dict(
        ps=':(literal)a?a/a.JPG',
        fordir={
            None: dict(
                match=['a?a/a.JPG'],
            ),
            "a?a": dict(
                specs=[':(literal)a.JPG'],
                match=['a.JPG'],
            ),
        },
    ),
    dict(
        ps=':(literal,icase)SuB/A?A/a.jpg',
        fordir={
            None: {'match': ['sub/a?a/a.JPG']},
            "sub/a?a": {
                'specs': [':(literal,icase)a.jpg'],
                # MIH would really expect to following,
                # but it is not coming :(
                #'match': ['a.JPG'],
                'match': [],
            },
        },
    ),
    # invalid
    #
    # conceptual conflict and thereby unsupported by Git
    # makes sense and is easy to catch that
    dict(ps=':(glob,literal)broken', raises=ValueError),
]


def test_pathspecs(pathspec_match_testground):
    tg = pathspec_match_testground

    for testcase in testcases:
        if testcase.get('raises'):
            # test case states how `GitPathSpec` will blow up
            # on this case. Verify and skip any further testing
            # on this case
            with pytest.raises(testcase['raises']):
                GitPathSpec.from_pathspec_str(testcase['ps'])
            continue
        # create the instance
        ps = GitPathSpec.from_pathspec_str(testcase['ps'])
        # if no deviating normalized representation is given
        # it must match the original one
        assert str(ps) == testcase.get('norm', testcase['ps'])
        # test translations onto subdirs now
        # `None` is a special subdir that means "self", i.e.
        # not translation other than normalization, we can use it
        # to test matching behavior of the full pathspec
        for subdir, target in testcase.get('fordir', {}).items():
            # translate -- a single input pathspec can turn into
            # multiple translated ones. This is due to
            subdir_specs = [str(s) for s in ps.for_subdir(subdir)]
            if 'specs' in target:
                assert set(subdir_specs) == set(target['specs'])
            if 'match' in target:
                tg_subdir = tg / subdir if subdir else tg
                assert _list_files(tg_subdir, subdir_specs) == target['match']
