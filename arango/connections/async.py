from __future__ import absolute_import, unicode_literals

from functools import partial

from arango.exceptions import ArangoError
from arango.jobs import AsyncJob
from arango.connections import BaseConnection


class AsyncExecution(BaseConnection):
    """ArangoDB asynchronous execution.

    API requests via this class are placed in a server-side in-memory task
    queue and executed asynchronously in a fire-and-forget style.

    :param connection: ArangoDB database connection
    :type connection: arango.connection.Connection
    :param return_result: if ``True``, an :class:`arango.async.AsyncJob`
        instance (which holds the result of the request) is returned each
        time an API request is queued, otherwise ``None`` is returned
    :type return_result: bool

    .. warning::
        Asynchronous execution is currently an experimental feature and is not
        thread-safe.
    """

    def __init__(self, connection, return_result=True):
        super(AsyncExecution, self).__init__(
            protocol=connection.protocol,
            host=connection.host,
            port=connection.port,
            username=connection.username,
            password=connection.password,
            http_client=connection.http_client,
            database=connection.database,
            enable_logging=connection.logging_enabled,
            logger=connection.logger,
            async_ready=connection.async_ready
        )
        self._return_result = return_result
        self._type = 'async'
        self._parent = connection

    def __repr__(self):
        return '<ArangoDB asynchronous execution>'

    def handle_request(self, request, handler, job_class=None):
        """Handle the incoming request and response handler.

        :param request: the API request to be placed in the server-side queue
        :type request: arango.request.Request
        :param handler: the response handler
        :type handler: callable
        :param job_class: required to maintain compatibility with the
        BaseConnection interface, but should be None
        :returns: the async job or None
        :rtype: arango.async.AsyncJob
        :raises arango.exceptions.AsyncExecuteError: if the async request
            cannot be executed
        """

        if job_class is not None:
            raise ArangoError('async cannot called with a job_class other '
                              'than none')

        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = 'true'

        partially_applied_job = partial(AsyncJob,
                                        connection=self._parent,
                                        return_result=self._return_result)

        return BaseConnection.handle_request(self,
                                             request,
                                             handler,
                                             partially_applied_job)
