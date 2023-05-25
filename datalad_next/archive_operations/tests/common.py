from __future__ import annotations

from datalad_next.archive_operations import ArchiveOperations
from datalad_next.iter_collections.tests.utils import TestCollection


def check_basic_functionality(operations_class: type[ArchiveOperations],
                              test_collection: TestCollection,
                              example_member_name: str,
                              ):
    with operations_class(test_collection.path) as operations:
        with operations.open(example_member_name) as member:
            assert member.read() == test_collection.content

    operations = operations_class(test_collection.path)
    included = example_member_name in operations
    assert included is True
    items = list(operations)
    assert len(items) == test_collection.item_count
    operations.close()
