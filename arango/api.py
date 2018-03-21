from __future__ import absolute_import, unicode_literals


class APIWrapper(object):
    """API wrapper which facilitates access to the executor.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    """

    def __init__(self, connection, executor):
        self._conn = connection
        self._executor = executor
        self._context = {
            'APIExecutor': 'default',
            'AsyncExecutor': 'async',
            'BatchExecutor': 'batch',
            'TransactionExecutor': 'transaction',
        }[self._executor.__class__.__name__]

    @property
    def context(self):
        """Return the API execution context (e.g. "async", "batch").

        :return: API execution context.
        :rtype: str or unicode
        """
        return self._context

    def _execute(self, request, response_handler):
        """Execute an API call.

        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        :rtype: dict or list or int or bool or str or unicode
        """
        return self._executor.execute(self._conn, request, response_handler)


class APIExecutor(object):
    """Base API executor.

    API executor dictates how an API request is executed depending on the
    context (e.g. async, batch). See :class:`arango.async.AsyncExecutor`
    or :class:`arango.batch.BatchExecutor` for more examples.
    """

    def execute(self, connection, request, response_handler):
        """Execute an API request.

        This method is meant to be overridden by subclasses and its behaviour
        re-defined depending on the execution context (e.g. async, batch). For
        more concrete examples, see :class:`arango.async.AsyncExecutor` or
        :class:`arango.batch.BatchExecutor`.

        :param connection: HTTP connection.
        :type connection: arango.connection.Connection
        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API call result or its future.
        :rtype: bool or list or dict or arango.connection.Future
        """
        response = connection.send_request(request)
        return response_handler(response)
