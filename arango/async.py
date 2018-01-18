from __future__ import absolute_import, unicode_literals

from arango.exceptions import (
    AsyncExecuteError
)
from arango.jobs import AsyncJob
from arango.requesters import Requester
from arango.utils import HTTP_OK


class AsyncRequester(Requester):
    """ArangoDB asynchronous API requester.

    API requests made via this class are placed in a server-side, in-memory
    task queue and executed asynchronously in a fire-and-forget style.

    :param requester: ArangoDB API requester object..
    :type requester: arango.requesters.Requester
    :param return_result: If set to True, an :class:`arango.async.AsyncJob`
        instance which holds the result of the request is returned whenever an
        API request is queued. If set to False, None is returned.
    :type return_result: bool

    .. warning::
        Asynchronous execution is currently an experimental feature and is not
        thread-safe.
    """

    def __init__(self, connection, return_result=True, lazy_load=True):
        super(AsyncRequester, self).__init__(
            protocol=connection.protocol,
            host=connection.host,
            port=connection.port,
            username=connection.username,
            password=connection.password,
            http_client=connection.http_client,
            database=connection.database,
            logger=connection.logger,
        )
        self._type = 'async'
        self._return_result = return_result
        self._lazy_load = lazy_load

    def execute_request(self, request, response_handler):
        """Make an asynchronous API request.

        :param request: The request to be placed in the server-side queue.
        :type request: arango.request.Request
        :param response_handler: The response handler function.
        :type response_handler: callable
        :return: The asynchronous job object.
        :rtype: arango.async.AsyncJob
        :raise arango.exceptions.AsyncExecuteError: If the execution fails.
        """
        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = 'true'

        res = self._execute_request(request)

        if res.status_code not in HTTP_OK:
            raise AsyncExecuteError(res)

        if not self._return_result:
            return None

        job_id = res.headers['x-arango-async-id']
        return AsyncJob(self, job_id, response_handler)


class AsyncJob(object):
    """ArangoDB async job which holds the result of an API request.

    An async job tracks the status of a queued API request and its result.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param job_id: The ID of the async job
    :type job_id: str | unicode
    :param handler: The response handler
    :type handler: callable
    """

    def __init__(self,
                 handler,
                 response=None,
                 connection=None,
                 return_result=True):
        BaseJob.__init__(self, handler, None,
                         job_id=None,
                         assign_id=False,
                         job_type='asynchronous')
        self._conn = connection
        self._initial_response = response
        self._result = None

        self._return_result = return_result

        if self._initial_response is None:
            raise ValueError('AsyncJob must be instantiated with a '
                             'response.')

        if self._conn is None:
            raise ValueError('AsyncJob must be instantiated with a '
                             'connection.')

        if not self._conn.async_ready:
            self.id

    @property
    def initial_response(self):
        if self._initial_response.status_code not in HTTP_OK:
            raise AsyncExecuteError(self._initial_response)
        return self._initial_response

    @property
    def id(self):
        """Return the UUID of the job.

        :return: The UUID of the job
        :rtype: str | unicode
        """
        if self._job_id is None:
            res = self.initial_response

            if self._return_result:
                self._job_id = res.headers['x-arango-async-id']

        return self._job_id

    def status(self):
        """Return the status of the async job from the server.

        :return: The status of the async job, which can be "pending" (the
            job is still in the queue), "done" (the job finished or raised
            an exception), or `"cancelled"` (the job was cancelled before
            completion)
        :rtype: str | unicode
        :raise arango.exceptions.AsyncJobStatusError: If the status of the
            async job cannot be retrieved from the server
        """

        request = Request(
            method='get',
            endpoint='/_api/job/{}'.format(self.id)
        )

        def handler(res):
            if res.status_code == 204:
                self.update('pending')
            elif res.status_code in HTTP_OK:
                self.update('done')
            elif res.status_code == 404:
                raise AsyncJobStatusError(res,
                                          'Job {} missing'.format(self.id))
            else:
                raise AsyncJobStatusError(res)

            return self._status

        response = self._conn.underlying._execute_request(request, handler,
                                                          job_class=BaseJob)

        return response.result(raise_errors=True)

    def result(self, raise_errors=False):
        """Return the result of the async job if available.

        :return: The result or the exception from the async job
        :rtype: object
        :raise arango.exceptions.AsyncJobResultError: If the result of the
            async job cannot be retrieved from the server

        .. note::
            An async job result will automatically be cleared from the server
            once fetched and will *not* be available in subsequent calls.
        """

        if not self._return_result:
            return None

        if self._result is None or isinstance(self._result, BaseException):
            request = Request(
                method='put',
                endpoint='/_api/job/{}'.format(self.id)
            )

            def handler(res):
                if res.status_code == 204:
                    raise AsyncJobResultError(
                        'Job {} not done'.format(self.id))
                elif res.status_code in HTTP_OK:
                    self.update('done', res)
                elif res.status_code == 404:
                    raise AsyncJobResultError(res, 'Job {} missing'.format(
                        self.id))
                else:
                    raise AsyncJobResultError(res)

                if ('X-Arango-Async-Id' in res.headers
                        or 'x-arango-async-id' in res.headers):
                    return self._handler(res)

            try:
                self._result = self._conn.underlying._execute_request(
                    request,
                    handler,
                    job_class=BaseJob
                ).result(raise_errors=True)

            except ArangoError as err:
                self.update('error')
                self._result = err

            if raise_errors:
                if isinstance(self._result, BaseException):
                    raise self._result

        return self._result

    def cancel(self, ignore_missing=False):  # pragma: no cover
        """Cancel the async job if it is still pending.

        :param ignore_missing: ignore missing async jobs
        :type ignore_missing: bool
        :return: True if the job was cancelled successfully, False if
            the job was not found but **ignore_missing** was set to True
        :rtype: bool
        :raise arango.exceptions.AsyncJobCancelError: If the async job cannot
            be cancelled

        .. note::
            An async job cannot be cancelled once it is taken out of the queue
            (i.e. started, finished or cancelled).
        """

        request = Request(
            method='put',
            endpoint='/_api/job/{}/cancel'.format(self.id)
        )

        def handler(res):
            if res.status_code == 200:
                self.update('cancelled')
                return True
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                raise AsyncJobCancelError(res,
                                          'Job {} missing'.format(self.id))
            else:
                raise AsyncJobCancelError(res)

        response = self._conn.underlying._execute_request(request, handler,
                                                          job_class=BaseJob)

        return response.result(raise_errors=True)

    def clear(self, ignore_missing=False):
        """Delete the result of the job from the server.

        :param ignore_missing: ignore missing async jobs
        :type ignore_missing: bool
        :return: True if the result was deleted successfully, False
            if the job was not found but **ignore_missing** was set to True
        :rtype: bool
        :raise arango.exceptions.AsyncJobClearError: If the result of the
            async job cannot be delete from the server
        """

        request = Request(
            method='delete',
            endpoint='/_api/job/{}'.format(self.id)
        )

        def handler(res):
            if res.status_code in HTTP_OK:
                return True
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                raise AsyncJobClearError(res,
                                         'Job {} missing'.format(self.id))
            else:
                raise AsyncJobClearError(res)

        response = self._conn.underlying._execute_request(request, handler,
                                                          job_class=BaseJob)

        return response.result(raise_errors=True)
