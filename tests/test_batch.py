from __future__ import absolute_import, unicode_literals

import mock
import pytest
from six import string_types

from arango.database import Database
from arango.exceptions import (
    DocumentRevisionError,
    DocumentInsertError,
    BatchExecuteError,
    BatchJobResultError,
    BatchBadStateError
)
from tests.utils import extract


# noinspection PyUnresolvedReferences
def test_batch_attributes(db, col, docs):
    with db.begin_batch() as batch:
        assert isinstance(batch.id, string_types)
        assert isinstance(batch.db, Database)
        assert batch.db.context == 'batch'
        assert batch.jobs == []
        assert batch.status == 'pending'
        job = batch.db.collection(col.name).insert_many(docs)

    assert batch.jobs == [job]
    assert batch.status == 'done'
    assert '<Batch {}>'.format(batch.id) == repr(batch)
    assert '<BatchJob {}>'.format(job.id) == repr(job)
    assert extract('_key', col.all()) == extract('_key', docs)


def test_batch_execute_without_result(db, col, docs):
    with db.begin_batch(return_result=False) as batch:
        batch_col = batch.db.collection(col.name)
        assert batch_col.insert(docs[0]) is None
        assert batch_col.delete(docs[0]) is None
        assert batch_col.insert(docs[1]) is None
        assert batch_col.delete(docs[1]) is None
        assert batch_col.insert(docs[2]) is None

    assert batch.jobs is None
    assert batch.status == 'done'
    assert extract('_key', col.all()) == extract('_key', [docs[2]])


def test_batch_execute_with_result(db, col, docs):
    with db.begin_batch(return_result=True) as batch:
        batch_col = batch.db.collection(col.name)
        job1 = batch_col.insert(docs[0])
        job2 = batch_col.insert(docs[1])
        job3 = batch_col.insert(docs[1])  # duplicate
        job4 = batch_col.get(document=docs[1], rev='9999')

    assert batch.jobs == [job1, job2, job3, job4]
    assert batch.status == 'done'

    assert extract('_key', col.all()) == extract('_key', docs[:2])

    # Test get successful results
    assert job1.status == 'done'
    assert job1.result()['_key'] == docs[0]['_key']
    assert job2.status == 'done'
    assert job2.result()['_key'] == docs[1]['_key']

    # Test get insert error with raise_errors on and off
    assert job3.status == 'done'
    with pytest.raises(DocumentInsertError):
        job3.result(raise_errors=True)
    assert isinstance(job3.result(), DocumentInsertError)

    # Test get revision error with raise_errors on and off
    assert job4.status == 'done'
    with pytest.raises(DocumentRevisionError):
        job4.result(raise_errors=True)
    assert isinstance(job4.result(), DocumentRevisionError)


def test_batch_empty_commit(db):
    batch = db.begin_batch()
    assert batch.status == 'pending'

    assert list(batch.commit()) == []
    assert batch.status == 'done'


def test_batch_double_commit(db, col):
    batch = db.begin_batch()
    batch.db.collection(col.name).insert({})
    assert batch.status == 'pending'

    # Test first commit
    batch.commit()
    assert batch.status == 'done'
    assert len(col) == 1
    random_doc = col.random()

    # Test second commit which should fail
    with pytest.raises(BatchBadStateError) as err:
        batch.commit()
    assert batch.status == 'done'
    assert len(col) == 1
    assert col.random() == random_doc
    assert 'committed already' in str(err.value)


def test_batch_action_after_commit(db, col):
    with db.begin_batch() as batch:
        batch.db.collection(col.name).insert({})

    # Test insert after the batch has been committed
    with pytest.raises(BatchBadStateError) as err:
        batch.db.collection(col.name).insert({})
    assert 'committed already' in str(err.value)
    assert len(col) == 1
    assert batch.status == 'done'


def test_batch_execute_error(bad_db, col, docs):
    batch = bad_db.begin_batch(return_result=True)
    batch.db.collection(col.name).insert_many(docs)
    assert batch.status == 'pending'

    # Test batch execute with bad credentials
    with pytest.raises(BatchExecuteError):
        batch.commit()
    assert len(col) == 0
    assert batch.status == 'done'


def test_batch_job_result_not_ready(db, col, docs):
    batch = db.begin_batch(return_result=True)
    job = batch.db.collection(col.name).insert_many(docs)
    assert batch.status == 'pending'

    # Test get job result before commit with raise_errors set to True
    with pytest.raises(BatchJobResultError):
        job.result(raise_errors=True)

    # Test get job result before commit with raise_errors set to False
    with pytest.raises(BatchJobResultError):
        job.result(raise_errors=False)

    # Test commit to make sure it still works after the errors
    assert list(batch.commit()) == [job]
    assert len(job.result()) == len(docs)
    assert extract('_key', col.all()) == extract('_key', docs)


def test_batch_bad_state(db, col, docs):
    batch = db.begin_batch()
    batch_col = batch.db.collection(col.name)
    batch_col.insert(docs[0])
    batch_col.insert(docs[1])
    batch_col.insert(docs[1])

    # Monkey patch the connection object
    mock_resp = mock.MagicMock()
    mock_resp.is_success = True
    mock_resp.raw_body = ''
    mock_send_request = mock.MagicMock()
    mock_send_request.return_value = mock_resp
    mock_connection = mock.MagicMock()
    mock_connection.send_request = mock_send_request
    batch._conn = mock_connection

    # Test commit with invalid batch state
    with pytest.raises(BatchBadStateError) as err:
        batch.commit()
    assert batch.status == 'done'
    assert 'expecting 3 parts in batch response but got 0' in str(err.value)
