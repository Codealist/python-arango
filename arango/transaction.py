from __future__ import absolute_import, unicode_literals

from uuid import uuid4

import arango.database
from arango.exceptions import (
    TransactionBadStateError,
    TransactionExecuteError,
    TransactionJobResultError,
    TransactionJobQueueError)
from arango.api import APIExecutor
from arango.request import Request
from arango.response import Response


class TransactionExecutor(APIExecutor):
    """Executes transaction API requests.

    :param transaction: Transaction object.
    :type transaction: arango.transaction.Transaction
    """

    def __init__(self, transaction):
        self._transaction = transaction

    # noinspection PyProtectedMember
    def execute(self, _, request, response_handler):
        return self._transaction._add_request(request, response_handler)


class Transaction(object):
    """Transaction state.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param timeout: Timeout on collection locks.
    :type timeout: int
    :param sync: Block until the operation is synchronized to disk.
    :type sync: bool
    :param return_result: If set to True, API requests are queued client-side
        and :class:`arango.transaction.TransactionJob` instances are returned
        to user. Job instances are populated with the results on commit. If set
        to False, requests are queued and executed, but results are not saved
        and job objects are not returned to the user.
    :type return_result: bool
    """

    def __init__(self,
                 connection,
                 timeout=None,
                 sync=None,
                 return_result=True):
        self._id = uuid4().hex
        self._status = 'pending'
        self._conn = connection
        self._timeout = timeout
        self._sync = sync
        self._return_result = return_result
        self._queue = []
        self._executor = TransactionExecutor(self)
        self._database = arango.database.Database(self._conn, self._executor)

    def __repr__(self):
        return '<Transaction {}>'.format(self._id)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None:
            self.commit()

    def _verify_no_commit(self):
        if self._status == 'done':
            raise TransactionBadStateError(
                message='transaction {} committed already.'.format(self._id))

    def _add_request(self, request, response_handler):
        self._verify_no_commit()
        if request.command is None:
            raise TransactionJobQueueError(
                message='method not allowed in transactions')

        job = TransactionJob(response_handler)
        self._queue.append((request, job))
        return job if self._return_result else None

    @property
    def id(self):
        """Return the transaction ID.

        :return: Transaction ID.
        :rtype: str | unicode
        """
        return self._id

    @property
    def status(self):
        """Return the transaction status.

        If the transaction is not committed, the status is set to "pending".
        If it is committed, the status is changed to "done".

        :return: Batch status.
        :rtype: str | unicode
        """
        return self._status

    @property
    def timeout(self):
        return self._timeout

    @property
    def sync(self):
        return self._sync

    @property
    def db(self):
        """Return the database wrapper for transaction.

        :return: Database wrapper.
        :rtype: arango.database.Database
        """
        return self._database

    @property
    def jobs(self):
        """Return the jobs in this transaction.

        :return: Transaction jobs.
        :rtype: [arango.transaction.TransactionJob]
        """
        if not self._return_result:
            return None
        return [job for _, job in self._queue]

    # noinspection PyProtectedMember
    def commit(self):
        """Execute the queued requests in a transaction.

        If **return_result** was set to True during the initialization of the
        executor, the :class:`arango.transaction.TransactionJob` instances
        are automatically populated with the results.

        :return: List of transaction jobs.
        :rtype: [arango.transaction.TransactionJob]

        :raise arango.exceptions.TransactionExecuteError: If commit fails.
        """
        self._verify_no_commit()
        self._status = 'done'
        if len(self._queue) == 0:
            return self.jobs

        write_collections = set()
        read_collections = set()

        # Buffer for building the transaction command
        cmd_buffer = ['var db = require("internal").db', 'var result = {}']

        # Build the transaction request payload from the queued jobs
        for req, job in self._queue:
            if req.write is not None:
                write_collections.add(req.write)
            if req.read is not None:
                read_collections.add(req.read)
            cmd_buffer.append('result["{}"] = {}'.format(job.id, req.command))
        cmd_buffer.append('return result;')

        data = {
            'action': 'function () {{ {} }}'.format(';'.join(cmd_buffer)),
            'collections': {
                'read': list(read_collections),
                'write': list(write_collections)
            }
        }
        if self._timeout is not None:
            data['lockTimeout'] = self._timeout
        if self._sync is not None:
            data['waitForSync'] = self._sync

        request = Request(
            method='post',
            endpoint='/_api/transaction',
            data=data,
        )
        resp = self._conn.send_request(request)

        if not resp.is_success:
            raise TransactionExecuteError(resp)
        if not self._return_result:
            return None
        result = resp.body['result']
        for req, job in self._queue:
            job._response = Response(
                method=req.method,
                url=self._conn.url_prefix + req.endpoint,
                headers={},
                status_code=200,
                status_text='OK',
                raw_body=result.get(job.id)
            )
            job._status = 'done'
        return self.jobs


class TransactionJob(object):
    """Transaction API call job.

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
        """Return the job ID.

        :return: Job ID.
        :rtype: str or unicode
        """
        return self._id

    @property
    def status(self):
        """Return the transaction job status.

        :return: Transaction job status.
        :rtype: str or unicode
        """
        return self._status

    def result(self, raise_errors=False):
        """Return the result of the transaction job is available.

        :param raise_errors: If set to True, any exception raised during the
            job execution is propagated up. If set to False, the exception is
            not raised but returned as an object.
        :type raise_errors: bool
        :return: Transaction job result.
        :rtype: object
        :raise arango.exceptions.TransactionJobResultError: If result is not
            available. For example, the transaction was not committed yet.
        :raise arango.exceptions.ArangoError: If **raise_errors* was set to
            True and the execution failed, the exception is propagated up.
        """
        if self._status == 'pending':
            raise TransactionJobResultError(message='result not available yet')
        try:
            result = self._response_handler(self._response)
        except Exception as error:
            if raise_errors:
                raise
            return error
        else:
            return result
