from __future__ import absolute_import, unicode_literals

from functools import partial

from arango.exceptions import ArangoError
from arango.jobs import AsyncJob
from arango.requesters import Requester


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

    def __init__(self, connection, return_result=True):
        super(AsyncRequester, self).__init__(
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

    def execute_request(self, request, response_handler):
        """Make an asynchronous API request.

        :param request: The request to be placed in the server-side queue.
        :type request: arango.request.Request
        :param response_handler: The response handler function.
        :type response_handler: callable
        :return: The asynchronous job object.
        :rtype: arango.async.AsyncJob
        :raise arango.exceptions.AsyncExecuteError: If the request cannot be
            made.
        """
        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = 'true'

        job = partial(AsyncJob, connection=self._parent, return_result=self._return_result)

        return Requester.execute_request(self, request, response_handler)
