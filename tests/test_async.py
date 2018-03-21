from __future__ import absolute_import, unicode_literals

import time

import pytest
from six import string_types

from arango.async import AsyncJob
from arango.database import Database
from arango.exceptions import (
    AsyncExecuteError,
    AsyncJobClearError,
    AsyncJobResultError,
    AsyncJobStatusError,
    AsyncJobListError,
    AsyncJobCancelError,
    AQLQueryExecuteError
)
from tests.utils import extract


def wait_for_job(job):
    while job.status() != 'done':
        time.sleep(.05)
    return job


def wait_for_all_jobs(db):
    while len(db.async_jobs('pending')) > 0:
        time.sleep(.05)


def test_async_attributes(db, col):
    async_db = db.begin_async()
    assert isinstance(async_db, Database)
    assert async_db.context == 'async'

    async_col = async_db.collection(col.name)
    assert async_col.context == 'async'

    async_aql = async_db.aql
    assert async_aql.context == 'async'
    job = async_aql.execute('THIS IS AN INVALID QUERY')
    assert isinstance(job, AsyncJob)
    assert isinstance(job.id, string_types)
    assert '<AsyncJob {}>'.format(job.id) == repr(job)


def test_async_execute_without_result(db, col, docs):
    # Insert test documents asynchronously with return_result set to False
    async_col = db.begin_async(return_result=False).collection(col.name)

    # Ensure that no jobs were returned
    assert async_col.insert(docs[0]) is None
    assert async_col.insert(docs[1]) is None
    assert async_col.insert(docs[2]) is None

    # Ensure that the operations went through
    wait_for_all_jobs(db)
    assert extract('_key', col.all()) == ['1', '2', '3']


def test_async_execute_aql_query(db, col, docs):
    col.import_bulk(docs)
    async_db = db.begin_async(return_result=True)

    # Test async execution of an bad AQL query with raise_errors set to False
    job = wait_for_job(async_db.aql.execute('THIS IS AN INVALID QUERY'))
    assert isinstance(job.result(), AQLQueryExecuteError)

    # Test async execution of an bad AQL query with raise_errors set to True
    job = wait_for_job(async_db.aql.execute('THIS IS AN INVALID QUERY'))
    with pytest.raises(AQLQueryExecuteError):
        job.result(raise_errors=True)

    # Test async execution of a valid AQL query
    job = wait_for_job(async_db.aql.execute(
        'FOR d IN {} FILTER d.val < 4 RETURN d'.format(col.name),
    ))
    assert extract('_key', job.result()) == ['1', '2', '3']


def test_async_get_job_status(db, bad_db, col):
    async_col = db.begin_async(return_result=True).collection(col.name)
    docs = [{'_key': str(i), 'val': i} for i in range(10000)]

    # Test get status of a pending job
    job = async_col.insert_many(docs, sync=True)
    assert job.status() == 'pending'

    # Test get status of a finished job
    assert wait_for_job(job).status() == 'done'
    assert len(job.result()) == len(docs)

    # Test get status of a missing job
    with pytest.raises(AsyncJobStatusError) as err:
        job.status()
    assert err.value.error_code == 404
    assert '{} not found'.format(job.id) in str(err.value)

    # Test get status with bad credentials
    bad_job = wait_for_job(async_col.insert_many(docs, sync=True))
    bad_job._conn = bad_db._conn
    with pytest.raises(AsyncJobStatusError):
        bad_job.status()


def test_async_get_job_result(db, bad_db, col):
    async_col = db.begin_async(return_result=True).collection(col.name)
    docs = [{'_key': str(i), 'val': i} for i in range(10000)]

    job1 = async_col.insert_many(docs, sync=True)
    job2 = async_col.insert_many(docs, sync=True)
    job3 = async_col.insert_many(docs, sync=True)

    # Test get result from a pending job
    with pytest.raises(AsyncJobResultError) as err:
        job3.result(raise_errors=True)
    assert '{} not done'.format(job3.id) in str(err.value)

    # Test get result from finished jobs
    for job in [job1, job2, job3]:
        assert len(wait_for_job(job).result(raise_errors=True)) == len(docs)
    assert len(col) == len(docs)

    # Test get result from cleared jobs
    for job in [job1, job2, job3]:
        with pytest.raises(AsyncJobResultError) as err:
            job.result()
        assert '{} not found'.format(job.id) in str(err.value)

    # Test get result from invalid job
    bad_job = async_col.insert_many(docs, sync=True)
    bad_job._conn = bad_db._conn
    with pytest.raises(AsyncJobResultError):
        bad_job.result()


def test_async_cancel_job(db, bad_db):
    async_db = db.begin_async(return_result=True)

    # Queue a long running request to ensure that next job is cancellable
    job = async_db.aql.execute('RETURN SLEEP(5)')

    # Test cancel a pending job
    assert job.cancel() is True

    # Test cancel a missing job
    job._id = 'invalid_id'
    with pytest.raises(AsyncJobCancelError) as err:
        job.cancel(ignore_missing=False)
    assert '{} not found'.format(job.id) in str(err.value)
    assert job.cancel(ignore_missing=True) is False

    # Test cancel with bad credentials
    bad_job = async_db.aql.execute('RETURN SLEEP(5)')
    bad_job._conn = bad_db._conn
    with pytest.raises(AsyncJobCancelError):
        bad_job.cancel()


def test_async_clear_job(db, bad_db, col, docs):
    # Setup test asynchronous jobs
    async_col = db.begin_async(return_result=True).collection(col.name)

    job = wait_for_job(async_col.insert(docs[0]))

    # Test clear finished job
    assert job.clear(ignore_missing=True) is True

    # Test clear missing job
    with pytest.raises(AsyncJobClearError) as err:
        job.clear(ignore_missing=False)
    assert '{} not found'.format(job.id) in str(err.value)
    assert job.clear(ignore_missing=True) is False

    # Test clear with bad credentials
    job._conn = bad_db._conn
    with pytest.raises(AsyncJobClearError):
        job.clear()


def test_async_execute_errors(bad_db, col, docs):
    bad_async_db = bad_db.begin_async(return_result=False)

    with pytest.raises(AsyncExecuteError):
        bad_async_db.collection(col.name).insert(docs[0])

    with pytest.raises(AsyncExecuteError):
        bad_async_db.collection(col.name).properties()

    with pytest.raises(AsyncExecuteError):
        bad_async_db.aql.execute('FOR d IN {} RETURN d'.format(col.name))


def test_async_clear_jobs(db, bad_db, col, docs):
    async_col = db.begin_async(return_result=True).collection(col.name)

    job1 = wait_for_job(async_col.insert(docs[0]))
    job2 = wait_for_job(async_col.insert(docs[1]))
    job3 = wait_for_job(async_col.insert(docs[2]))

    # Test clear all async jobs
    assert db.clear_async_jobs() is True
    for job in [job1, job2, job3]:
        with pytest.raises(AsyncJobStatusError) as err:
            job.status()
        assert '{} not found'.format(job.id) in str(err.value)

    # Set up test documents again
    job1 = wait_for_job(async_col.insert(docs[0]))
    job2 = wait_for_job(async_col.insert(docs[1]))
    job3 = wait_for_job(async_col.insert(docs[2]))

    # Test clear jobs that have expired
    past = int(time.time()) - 1000000
    assert db.clear_async_jobs(threshold=past) is True
    for job in [job1, job2, job3]:
        assert job.status() == 'done'

    # Test clear jobs that have not expired yet
    future = int(time.time()) + 1000000
    assert db.clear_async_jobs(threshold=future) is True
    for job in [job1, job2, job3]:
        with pytest.raises(AsyncJobStatusError) as err:
            job.status()
        assert '{} not found'.format(job.id) in str(err.value)

    # Test clear job without authentication
    with pytest.raises(AsyncJobClearError):
        bad_db.clear_async_jobs()


def test_async_list_jobs(db, col, docs):
    db.clear_async_jobs()
    async_db = db.begin_async(return_result=True)
    async_col = async_db.collection(col.name)

    job1 = wait_for_job(async_col.insert(docs[0]))
    job2 = wait_for_job(async_col.insert(docs[1]))
    job3 = wait_for_job(async_col.insert(docs[2]))

    # Test list async jobs that are done
    job_ids = set(db.async_jobs(status='done'))
    assert job1.id in job_ids
    assert job2.id in job_ids
    assert job3.id in job_ids

    # Test list async jobs that are pending
    job4 = async_db.aql.execute('RETURN SLEEP(5)')
    assert db.async_jobs(status='pending') == [job4.id]

    # Test list async jobs with invalid status
    with pytest.raises(AsyncJobListError):
        db.async_jobs(status='bad_status')

    # Test list jobs with count
    job_ids = db.async_jobs(status='done', count=1)
    assert len(job_ids) == 1
    assert job_ids[0] in [job1.id, job2.id, job3.id]
