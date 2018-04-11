from __future__ import absolute_import, unicode_literals

from uuid import uuid4

from arango.exceptions import (
    AsyncJobCancelError,
    AsyncJobStatusError,
    AsyncJobResultError,
    AsyncJobClearError,
    BatchJobResultError,
    TransactionJobResultError,
)
from arango.request import Request


class Job(object):  # pragma: no cover
    """API execution job base class.

    Jobs are used to track progress, and retrieve result of API executions.
    """

    @property
    def id(self):
        """Return the job ID.

        :return: Job ID.
        :rtype: str | unicode
        """
        raise NotImplementedError

    def status(self):
        """Return the job status.

        :return: Job status.
        :rtype: str | unicode
        """
        raise NotImplementedError

    def result(self):
        """Return the job result if available.

        :return: Job result.
        :rtype: str | unicode | bool | int | list | dict
        :raise arango.exceptions.ArangoError: If result is an error.
        """
        raise NotImplementedError


class AsyncJob(Job):
    """Async API execution job.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param job_id: Async job ID.
    :type job_id: str | unicode
    :param response_handler: Response handler.
    :type response_handler: callable
    """

    __slots__ = ['_conn', '_id', '_response_handler']

    def __init__(self, connection, job_id, response_handler):
        self._conn = connection
        self._id = job_id
        self._response_handler = response_handler

    def __repr__(self):
        return '<AsyncJob {}>'.format(self._id)

    @property
    def id(self):
        """Return the async job ID.

        :return: Async job ID.
        :rtype: str | unicode
        """
        return self._id

    def status(self):
        """Return the async job status from the server.

        :return: Async job status which can be one of the following: "pending"
            (job still in queue), "done" (job finished or raised an exception),
            or "cancelled" (job cancelled before completion).
        :rtype: str | unicode
        :raise arango.exceptions.AsyncJobStatusError: If retrieval fails.

        .. note::
            Once job result is retrieved via func:`arango.job.AsyncJob.result`
            method, it is deleted from the server and subsequent queries will
            fail.
        """
        request = Request(
            method='get',
            endpoint='/_api/job/{}'.format(self._id)
        )
        resp = self._conn.send_request(request)
        if resp.status_code == 204:
            return 'pending'
        elif resp.is_success:
            return 'done'
        elif resp.error_code == 404:
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobStatusError(resp, error_message)
        else:
            raise AsyncJobStatusError(resp)

    def result(self):
        """Return the async job result from the server.

        If the job raised an exception, it is propagated up here.

        :return: Async job result.
        :rtype: str | unicode | bool | int | list | dict
        :raise arango.exceptions.ArangoError: If the job raised an exception.
        :raise arango.exceptions.AsyncJobResultError: If retrieval fails.

        .. note::
            Once job result is retrieved, it is deleted from the server and
            subsequent queries will fail.
        """
        request = Request(
            method='put',
            endpoint='/_api/job/{}'.format(self._id)
        )
        resp = self._conn.send_request(request)
        headers = resp.headers
        if 'X-Arango-Async-Id' in headers or 'x-arango-async-id' in headers:
            return self._response_handler(resp)
        if resp.status_code == 204:
            error_message = 'job {} not done'.format(self._id)
            raise AsyncJobResultError(resp, error_message)
        elif resp.error_code == 404:
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobResultError(resp, error_message)
        else:
            raise AsyncJobResultError(resp)

    def cancel(self, ignore_missing=False):
        """Cancel the async job if it is still pending.

        :param ignore_missing: Do not raise an exception on missing job.
        :type ignore_missing: bool
        :return: True if the job was cancelled successfully, False if the job
            was not found but **ignore_missing** was set to True.
        :rtype: bool
        :raise arango.exceptions.AsyncJobCancelError: If cancel fails.

        .. note::
            An async job cannot be cancelled once it is out of the queue.
        """
        request = Request(
            method='put',
            endpoint='/_api/job/{}/cancel'.format(self._id)
        )
        resp = self._conn.send_request(request)
        if resp.status_code == 200:
            return True
        elif resp.error_code == 404:
            if ignore_missing:
                return False
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobCancelError(resp, error_message)
        else:
            raise AsyncJobCancelError(resp)

    def clear(self, ignore_missing=False):
        """Delete the job result from the server.

        :param ignore_missing: Do not raise an exception on missing job.
        :type ignore_missing: bool
        :return: True if the result was deleted successfully, False if the job
            was not found but **ignore_missing** was set to True.
        :rtype: bool
        :raise arango.exceptions.AsyncJobClearError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/job/{}'.format(self._id)
        )
        resp = self._conn.send_request(request)
        if resp.is_success:
            return True
        elif resp.error_code == 404:
            if ignore_missing:
                return False
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobClearError(resp, error_message)
        else:
            raise AsyncJobClearError(resp)


class BatchJob(Job):
    """Batch API job for retrieving the result.

    :param response_handler: HTTP response handler.
    :type response_handler: callable
    """

    __slots__ = ['_id', '_status', '_response', '_response_handler']

    def __init__(self, response_handler):
        self._id = uuid4().hex
        self._status = 'pending'
        self._response = None
        self._response_handler = response_handler

    def __repr__(self):
        return '<BatchJob {}>'.format(self._id)

    @property
    def id(self):
        """Return the batch job ID.

        :return: Batch job ID.
        :rtype: str | unicode
        """
        return self._id

    def status(self):
        """Return the batch job status.

        :return: Batch job status which can be one of the following: "pending"
            (waiting for batch to be committed) or "done" (batch was committed
            and result is available).
        :rtype: str | unicode
        """
        return self._status

    def result(self):
        """Return the batch job result.

        If the job raised an exception, it is propagated up here.

        :return: Batch job result.
        :rtype: str | unicode | bool | int | list | dict
        :raise arango.exceptions.ArangoError: If the job raised an exception.
        :raise arango.exceptions.BatchJobResultError: If job result is not
            available (i.e. batch not committed yet).
        """
        if self._status == 'pending':
            raise BatchJobResultError('result not available yet')
        return self._response_handler(self._response)


class TransactionJob(Job):
    """Transaction API execution job.

    :param response_handler: HTTP response handler.
    :type response_handler: callable
    """

    __slots__ = ['_id', '_status', '_response', '_response_handler']

    def __init__(self, response_handler):
        self._id = uuid4().hex
        self._status = 'pending'
        self._response = None
        self._response_handler = response_handler

    def __repr__(self):
        return '<TransactionJob {}>'.format(self._id)

    @property
    def id(self):
        """Return the transaction job ID.

        :return: Transaction job ID.
        :rtype: str | unicode
        """
        return self._id

    def status(self):
        """Return the transaction job status.

        :return: Transaction job status which can be one of the following:
            "pending" (waiting for transaction to be committed) or "done"
            (transaction was committed and result is available).
        :rtype: str | unicode
        """
        return self._status

    def result(self):
        """Return the transaction job result.

        :return: Transaction job result.
        :rtype: str | unicode | bool | int | list | dict
        :raise arango.exceptions.ArangoError: If the job raised an exception.
        :raise arango.exceptions.TransactionJobResultError: If job result is
            not available (i.e. transaction not committed yet).
        """
        if self._status == 'pending':
            raise TransactionJobResultError('result not available yet')
        return self._response_handler(self._response)
