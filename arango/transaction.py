from __future__ import absolute_import, unicode_literals

from uuid import uuid4
from collections import deque

from arango.requesters.base import Requester
from arango import Request
from arango.utils import HTTP_OK
from arango.exceptions import TransactionError


class Transaction(Requester):
    """ArangoDB transaction object.

    API requests made in a transaction are queued in memory and executed as a
    whole in a single HTTP call to ArangoDB server.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param read: The name(s) of the collection(s) to read from
    :type read: str | unicode | list
    :param write: The name(s) of the collection(s) to write to
    :type write: str | unicode | list
    :param sync: Block until the operation is synchronized to disk.
    :type sync: bool
    :param timeout: timeout on the collection locks
    :type timeout: int
    :param commit_on_error: only applicable when *context managers* are used
        to execute the transaction: If set to True, the requests queued so
        far are committed even if an exception is raised before exiting out of
        the context
    :type commit_on_error: bool

    .. note::
        Only writes are possible at the moment in a transaction.
    """

    def __init__(self,
                 requester,
                 read=None,
                 write=None,
                 sync=None,
                 timeout=None,
                 commit_on_error=False):
        super(Transaction, self).__init__(
            protocol=requester.protocol,
            host=requester.host,
            port=requester.port,
            username=requester.username,
            password=requester.password,
            http_client=requester.http_client,
            database=requester.database,
        )
        self._type = 'transaction'
        self._id = uuid4().hex
        self._actions = []
        self._collections = {}
        if read is not None:
            self._collections['read'] = read
        if write is not None:
            self._collections['write'] = write
        self._sync = sync
        self._timeout = timeout
        self._commit_on_error = commit_on_error
        self._parent = requester

    def __repr__(self):
        return '<ArangoDB transaction {}>'.format(self._id)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None or self._commit_on_error:
            return self.commit()

    @property
    def id(self):
        """Return the UUID of the transaction.

        :return: The UUID of the transaction
        :rtype: str | unicode
        """
        return self._id

    def execute_request(self, request, response_handler):
        """Handle the incoming request and response handler.

        :param request: The API request queued as part of the transaction, and
            executed only when the current transaction is committed via method
            :func:`arango.transaction.Transaction.commit`.
        :type request: arango.request.Request
        :param response_handler: The response handler.
        :type response_handler: callable
        """
        if request.command is None:
            raise TransactionError('The method does not support transactions.')
        self._actions.append(request.command)

    def commit(self):
        """Execute the queued API requests in one atomic step.

        :return: The result of the transaction.
        :rtype: arango.jobs.Job
        :raise arango.exceptions.TransactionError: If the commit fails.
        """
        action_results = ['a' + uuid4().hex for _ in self._actions]

        action_strings = deque()
        action_strings.append('db = require("internal").db;\n')

        for i in range(len(self._actions)):
            action_strings.append('var ')
            action_strings.append(action_results[i])
            action_strings.append(' = ')
            action_strings.append(self._actions[i])
            action_strings.append(';\n')

        action_strings.append('return [')
        for label in action_results:
            action_strings.append(label)
            action_strings.append(', ')

        if len(action_results) > 0:
            action_strings.pop()

        action_strings.append('];\n')

        action = ''.join(action_strings)

        request = Request(
            method='post',
            endpoint='/_api/transaction',
            data={
                'collections': self._collections,
                'action': 'function () {{ {} }}'.format(action)
            },
            params={
                'lockTimeout': self._timeout,
                'waitForSync': self._sync,
            }
        )

        self._actions = ['db = require("internal").db']

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise TransactionError(res)
            return res.body.get('result')

        return self._execute_request(request, response_handler)

    def execute(self, command, params=None, sync=None, timeout=None):
        """Execute raw Javascript code in this transaction.

        :param command: The raw Javascript code.
        :type command: str | unicode
        :param params: Optional arguments passed into the code.
        :type params: dict
        :param sync: Block until the operation is synchronized to disk. This
            overrides the **sync** value set during the initialization of the
            transaction object.
        :type sync: bool
        :param timeout: Timeout on collection locks. This overrides the value
            value set during the initialization of the transaction object.
        :type timeout: int
        :return: The result of the transaction.
        :rtype: dict
        :raise arango.exceptions.TransactionError: If the transaction fails.
        """
        data = {
            'collections': self._collections,
            'action': command
        }

        if timeout is None:
            timeout = self._timeout
        if timeout is not None:
            data['lockTimeout'] = timeout

        if sync is None:
            sync = self._sync
        if sync is not None:
            data['waitForSync'] = sync

        if params is not None:
            data['params'] = params

        request = Request(
            method='post',
            endpoint='/_api/transaction',
            data=data
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise TransactionError(res)
            return res.body.get('result')

        return self._execute_request(request, response_handler)
