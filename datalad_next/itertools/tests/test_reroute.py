
from more_itertools import intersperse

from ..reroute import (
    route_in,
    route_out,
    StoreOnly
)


def test_route_around():
    """Test routing of data around a consumer"""

    # Route 0 around `lambda x: 2.0 / x.
    store = list()
    r = route_in(
        map(
            lambda divisor: 2.0 / divisor,
            route_out(
                intersperse(0, range(2, 20)),
                store,
                lambda divisor: (StoreOnly, [divisor])
                                if divisor == 0
                                else (divisor, None)
            )
        ),
        store,
        lambda processed_data, stored_data: processed_data
                                            if processed_data is not StoreOnly
                                            else 'divisor is 0'
    )
    # The result should be a list in which every odd element consists of a list
    # with the elements `[n / 2.0, n]` and every even element consists of a list
    # with `[dont_process, 0]`, because the `0`s were routed around the
    # consumer, i.e. around `lambda x: x / 2.0`.
    assert list(r) == list(
        intersperse('divisor is 0', map(lambda x: 2.0 / x, range(2, 20)))
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
                lambda x: (StoreOnly, x)
            )
        ),
        store,
        lambda x, y: y
    )
    assert list(r) == list(range(10))
