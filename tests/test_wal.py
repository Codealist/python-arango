from __future__ import absolute_import, unicode_literals

import pytest

from arango.exceptions import (
    WALConfigureError,
    WALFlushError,
    WALPropertiesError,
    WALTransactionListError
)


def test_wal_get_properties(db, bad_db):
    properties = db.wal.properties()
    assert 'oversized_ops' in properties
    assert 'log_size' in properties
    assert 'historic_logs' in properties
    assert 'reserve_logs' in properties

    with pytest.raises(WALPropertiesError):
        bad_db.wal.properties()


def test_wal_set_properties(sys_db, bad_db):
    sys_db.wal.configure(
        historic_logs=15,
        oversized_ops=False,
        log_size=30000000,
        reserve_logs=5,
        throttle_limit=0,
        throttle_wait=16000
    )
    properties = sys_db.wal.properties()
    assert properties['historic_logs'] == 15
    assert properties['oversized_ops'] is False
    assert properties['log_size'] == 30000000
    assert properties['reserve_logs'] == 5
    assert properties['throttle_limit'] == 0
    assert properties['throttle_wait'] == 16000

    with pytest.raises(WALConfigureError):
        bad_db.wal.configure(log_size=2000000)


def test_wal_get_transactions(db, bad_db):
    result = db.wal.transactions()
    assert 'count' in result
    assert 'last_sealed' in result
    assert 'last_collected' in result

    with pytest.raises(WALTransactionListError):
        bad_db.wal.transactions()


def test_wal_flush(db, bad_db):
    result = db.wal.flush(garbage_collect=False, sync=False)
    assert isinstance(result, bool)

    with pytest.raises(WALFlushError):
        bad_db.wal.flush(garbage_collect=False, sync=False)
