
from more_itertools import intersperse

from ..reroute import (
    join_with_list,
    route_in,
    route_out,
    dont_process
)


def test_route_around():
    """Test routing of data around a consumer"""

    # Route 0 around `lambda x: x / 2.0`.
    store = list()
    r = route_in(
        map(
            lambda x: x / 2.0,
            route_out(
                intersperse(0, range(2, 20)),
                store,
                lambda x: (dont_process, [x]) if x == 0 else (x, [x])
            )
        ),
        store,
        join_with_list
    )
    # The result should be a list in which every odd element consists of a list
    # with the elements `[n / 2.0, n]` and every even element consists of a list
    # with `[dont_process, 0]`, because the `0`s were routed around the
    # consumer, i.e. around `lambda x: x / 2.0`.
    assert list(r) == list(
        intersperse([None, 0], map(lambda x: [x / 2.0, x], range(2, 20)))
    )


def test_route_no_processing():
    """Test routing of data without processing"""

    store = list()
    r = route_in(
        map(
            lambda x: x,
            route_out(
                range(10),
                store,
                lambda x: (dont_process, x)
            )
        ),
        store,
        lambda x, y: y
    )
    assert list(r) == list(range(10))
