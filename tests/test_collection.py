from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.exceptions import (
    CollectionChecksumError,
    CollectionConfigureError,
    CollectionLoadError,
    CollectionPropertiesError,
    CollectionRenameError,
    CollectionRevisionError,
    CollectionRotateJournalError,
    CollectionStatisticsError,
    CollectionTruncateError,
    CollectionUnloadError
)
from tests.utils import generate_collection_name


def test_collection_properties(col, bad_col):
    assert repr(col) == '<Collection {}>'.format(col.name)

    # Test get collection properties
    properties = col.properties()
    assert properties['name'] == col.name
    assert properties['system'] is False
    assert properties['edge'] is False

    # Test get collection properties with bad credentials
    with pytest.raises(CollectionPropertiesError):
        bad_col.properties()


def test_collection_configure(col, bad_col):
    prev_sync = col.properties()['sync']

    # Test configure
    properties = col.configure(
        sync=not prev_sync,
        journal_size=1000000
    )
    assert properties['name'] == col.name
    assert properties['system'] is False
    assert properties['edge'] is False
    assert properties['sync'] is not prev_sync

    # Test missing collection
    with pytest.raises(CollectionConfigureError):
        bad_col.configure(sync=True, journal_size=1000000)


def test_collection_rename(col, bad_col):
    assert col.name == col.name
    new_name = generate_collection_name()

    # Test rename collection
    assert col.rename(new_name) is True
    assert repr(col) == '<Collection {}>'.format(new_name)

    # Try again (the operation should be idempotent)
    assert col.rename(new_name) is True
    assert repr(col) == '<Collection {}>'.format(new_name)

    with pytest.raises(CollectionRenameError):
        bad_col.rename(new_name)


def test_collection_statistics(col, bad_col, docs):
    col.insert_many(docs)
    stats = col.statistics()
    assert 'documents_size' in stats

    with pytest.raises(CollectionStatisticsError):
        bad_col.statistics()

    revision = col.revision()
    assert isinstance(revision, string_types)
    with pytest.raises(CollectionRevisionError):
        bad_col.revision()


def test_collection_load(col, bad_col):
    assert col.load() is True
    with pytest.raises(CollectionLoadError):
        bad_col.load()


def test_collection_unload(col, bad_col):
    assert col.unload() is True
    with pytest.raises(CollectionUnloadError):
        bad_col.unload()


def test_collection_rotate(col, bad_col):
    assert isinstance(col.rotate(), bool)
    with pytest.raises(CollectionRotateJournalError):
        bad_col.rotate()


def test_collection_checksum(col, bad_col, docs):
    # Test checksum for an empty collection
    assert int(col.checksum(with_rev=True, with_data=False)) == 0
    assert int(col.checksum(with_rev=True, with_data=True)) == 0
    assert int(col.checksum(with_rev=False, with_data=False)) == 0
    assert int(col.checksum(with_rev=False, with_data=True)) == 0

    # Test checksum for a non-empty collection
    col.insert(docs[0])
    assert int(col.checksum(with_rev=True, with_data=False)) > 0
    assert int(col.checksum(with_rev=True, with_data=True)) > 0
    assert int(col.checksum(with_rev=False, with_data=False)) > 0
    assert int(col.checksum(with_rev=False, with_data=True)) > 0

    with pytest.raises(CollectionChecksumError):
        bad_col.checksum()


def test_collection_truncate(col, bad_col, docs):
    col.insert_many(docs)

    # Test preconditions
    assert len(col) == 6

    # Test truncate collection
    assert col.truncate() is True
    assert len(col) == 0

    with pytest.raises(CollectionTruncateError):
        bad_col.truncate()
