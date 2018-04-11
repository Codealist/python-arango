from __future__ import absolute_import, unicode_literals

__all__ = ['APIWrapper']


class APIWrapper(object):
    """API wrapper base class.

    This is a utility class which wraps the API executor.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.executor.DefaultExecutor
    """

    def __init__(self, connection, executor):
        self._conn = connection
        self._executor = executor
        self._is_transaction = self.context == 'transaction'

    @property
    def db_name(self):
        """Return the database name.

        :return: Database name.
        """
        return self._conn.db_name

    @property
    def username(self):
        """Return the username.

        :returns: Username.
        :rtype: str | unicode
        """
        return self._conn.username

    @property
    def context(self):
        """Return the API execution context.

        Possible values are "default", "async", "batch" and "transaction".

        :return: API execution context.
        :rtype: str | unicode
        """
        return self._executor.context

    def _execute(self, request, response_handler):
        """Execute an API according to the execution context.

        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        :rtype: str | unicode | bool | int | list | dict
        """
        return self._executor.execute(request, response_handler)
