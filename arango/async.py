from __future__ import absolute_import, unicode_literals

from arango.api import APIExecutor
from arango.exceptions import AsyncExecuteError
from arango.exceptions import (
    AsyncJobCancelError,
    AsyncJobStatusError,
    AsyncJobResultError,
    AsyncJobClearError
)
from arango.request import Request


class AsyncExecutor(APIExecutor):
    """Executes API requests asynchronously server-side.

    :param return_result: If set to True, API execution results are stored
        server-side and instances of :class:`arango.async.AsyncJob` are
        returned to user instead. If set to False, no results are stored
        and None is returned to the user.
    :type return_result: bool
    """

    def __init__(self, return_result):
        self._return_result = return_result

    def execute(self, connection, request, response_handler):
        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = 'true'

        resp = connection.send_request(request)
        if not resp.is_success:
            raise AsyncExecuteError(resp)
        if not self._return_result:
            return None

        job_id = resp.headers['x-arango-async-id']
        return AsyncJob(connection, job_id, response_handler)


class AsyncJob(object):
    """Allows tracking of an Async API execution and result retrieval.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param job_id: Job ID.
    :type job_id: str or unicode
    :param response_handler: Response handler.
    :type response_handler: callable
    """

    def __init__(self, connection, job_id, response_handler):
        self._conn = connection
        self._id = job_id
        self._response_handler = response_handler

    def __repr__(self):
        return '<AsyncJob {}>'.format(self._id)

    @property
    def id(self):
        """Return the job ID.

        :return: Job ID.
        :rtype: str or unicode
        """
        return self._id

    def status(self):
        """Return the job status from the server.

        :return: Job status which can be one of the following: "pending" (job
            is still in queue), "done" (job finished or raised an exception),
            or `"cancelled"` (job was cancelled before completion).
        :rtype: str or unicode
        :raise arango.exceptions.AsyncJobStatusError: If retrieval fails.

        .. note::
            Once the result of an API execution is retrieved from the server
            via func:`arango.async.AsyncJob.result` method, it is deleted from
            the server and subsequent requests will fail.
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
        elif resp.error_code == 404 or resp.status_code == 404:
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobStatusError(resp, error_message)
        else:
            raise AsyncJobStatusError(resp)

    def result(self, raise_errors=False):
        """Return the result of the async job if available.

        :param raise_errors: If set to True, any exception raised during the
            job execution is propagated up. If set to False, the exception is
            not raised but returned as an object.
        :type raise_errors: bool
        :return: Async job result.
        :rtype: object
        :raise arango.exceptions.AsyncJobResultError: If retrieval fails.
        :raise arango.exceptions.ArangoError: If **raise_errors* was set to
            True and the execution failed, the exception is propagated up.

        .. note::
            Once the result of an API execution is retrieved, it is deleted
            from the server and subsequent requests will fail.
        """
        request = Request(
            method='put',
            endpoint='/_api/job/{}'.format(self._id)
        )
        resp = self._conn.send_request(request)
        headers = resp.headers
        if 'X-Arango-Async-Id' in headers or 'x-arango-async-id' in headers:
            try:
                result = self._response_handler(resp)
            except Exception as error:
                if raise_errors:
                    raise
                return error
            else:
                return result
        if resp.status_code == 204:
            error_message = 'job {} not done'.format(self._id)
            raise AsyncJobResultError(resp, error_message)
        elif resp.error_code == 404 or resp.status_code == 404:
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobResultError(resp, error_message)
        else:
            raise AsyncJobResultError(resp)

    def cancel(self, ignore_missing=False):
        """Cancel the job if it is still pending.

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
        elif resp.error_code == 404 or resp.status_code == 404:
            if ignore_missing:
                return False
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobCancelError(resp, error_message)
        else:
            raise AsyncJobCancelError(resp)

    def clear(self, ignore_missing=False):
        """Delete the result from the server.

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
        elif resp.error_code == 404 or resp.status_code == 404:
            if ignore_missing:
                return False
            error_message = 'job {} not found'.format(self._id)
            raise AsyncJobClearError(resp, error_message)
        else:
            raise AsyncJobClearError(resp)
