from __future__ import absolute_import, unicode_literals

from six import string_types

__all__ = [
    'DefaultExecutor',
    'AsyncExecutor',
    'BatchExecutor',
    'TransactionExecutor'
]

from collections import OrderedDict
from uuid import uuid4

from arango.exceptions import (
    AsyncExecuteError,
    BatchStateError,
    BatchExecuteError,
    TransactionStateError,
    TransactionExecuteError,
)
from arango.job import (
    AsyncJob,
    BatchJob,
    TransactionJob
)
from arango.request import Request
from arango.response import Response
from arango.utils import suppress_warning


class Executor(object):  # pragma: no cover
    """API executor base class.

    API executors dictate how an API request is executed depending on the
    execution context (i.e. "default", "async", "batch", "transaction").

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    """
    context = None

    def __init__(self, connection):
        self._conn = connection

    def execute(self, request, response_handler):
        """Execute an API request in a standard fashion.

        This method is overridden for different execution contexts.

        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        :rtype: str | unicode | bool | int | list | dict
        """
        raise NotImplementedError


class DefaultExecutor(Executor):
    """Executes default API requests.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    """
    context = 'default'

    def __init__(self, connection):
        super(DefaultExecutor, self).__init__(connection)

    def execute(self, request, response_handler):
        """Execute an API request synchronously.

        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: API execution result.
        :rtype: str | unicode | bool | int | list | dict
        """
        response = self._conn.send_request(request)
        return response_handler(response)


class AsyncExecutor(DefaultExecutor):
    """Executes async API requests.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, result of an API execution is saved
        server-side, and an instance of :class:`arango.job.AsyncJob` is
        returned. If set to False, no results are saved and None is returned.
    :type return_result: bool
    """
    context = 'async'

    def __init__(self, connection, return_result):
        super(AsyncExecutor, self).__init__(connection)
        self._return_result = return_result

    def execute(self, request, response_handler):
        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = 'true'

        resp = self._conn.send_request(request)
        if not resp.is_success:
            raise AsyncExecuteError(resp)
        if not self._return_result:
            return None

        job_id = resp.headers['x-arango-async-id']
        return AsyncJob(self._conn, job_id, response_handler)


class BatchExecutor(DefaultExecutor):
    """Executes batch API requests.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`arango.job.BatchJob`. These job instances are populated with
        results on commit. If set to False, API executions return None instead
        and no results are saved client-side.
    :type return_result: bool
    """
    context = 'batch'

    def __init__(self, connection, return_result):
        super(BatchExecutor, self).__init__(connection)
        self._return_result = return_result
        self._queue = OrderedDict()
        self._committed = False

    def _verify_no_commit(self):
        """Verify that this batch has not been committed already.

        :raise arango.exceptions.BatchStateError: If batch is committed.
        """
        if self._committed:
            raise BatchStateError('batch already committed')

    @property
    def jobs(self):
        """Return the queued batch jobs.

        :return: Batch jobs or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [arango.job.BatchJob] | None
        """
        if not self._return_result:
            return None
        return [job for _, job in self._queue.values()]

    def execute(self, request, response_handler):
        """Put the request in the batch queue.

        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: Async job or None if **return_result** parameter was set to
            False during initialization.
        :rtype: arango.job.BatchJob | None
        :raise arango.exceptions.BatchStateError: If the batch was already
            committed.
        """
        self._verify_no_commit()

        job = BatchJob(response_handler)
        self._queue[job.id] = (request, job)
        return job if self._return_result else None

    def commit(self):
        """Execute the queued requests in a single batch API request.

        If **return_result** parameter was set to True during initialization,
        :class:`arango.job.BatchJob` instances are populated with results.

        :return: Batch jobs or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [arango.job.BatchJob] | None
        :raise arango.exceptions.BatchStateError: If batch state is invalid
            (e.g. batch was already committed or the response size did not
            match the expected).
        :raise arango.exceptions.BatchExecuteError: If commit fails.
        """
        self._verify_no_commit()
        self._committed = True

        if len(self._queue) == 0:
            return self.jobs

        # Boundary used for multipart request
        boundary = uuid4().hex

        # Buffer for building the batch request payload
        buffer = []
        for req, job in self._queue.values():
            buffer.append('--{}'.format(boundary))
            buffer.append('Content-Type: application/x-arango-batchpart')
            buffer.append('Content-Id: {}'.format(job.id))
            buffer.append('\r\n{}'.format(req))
        buffer.append('--{}--'.format(boundary))

        request = Request(
            method='post',
            endpoint='/_api/batch',
            headers={
                'Content-Type':
                    'multipart/form-data; boundary={}'.format(boundary)
            },
            data='\r\n'.join(buffer)
        )
        with suppress_warning('requests.packages.urllib3.connectionpool'):
            resp = self._conn.send_request(request)

        if not resp.is_success:
            raise BatchExecuteError(resp)

        if not self._return_result:
            return None

        raw_resps = resp.raw_body.split('--{}'.format(boundary))[1:-1]
        if len(self._queue) != len(raw_resps):
            raise BatchStateError(
                'expecting {} parts in batch response but got {}'
                .format(len(self._queue), len(raw_resps))
            )
        for raw_resp in raw_resps:
            # Parse and breakdown the batch response body
            resp_parts = raw_resp.strip().split('\r\n')
            raw_content_id = resp_parts[1]
            raw_body = resp_parts[-1]
            raw_status = resp_parts[3]
            job_id = raw_content_id.split(' ')[1]
            _, status_code, status_text = raw_status.split(' ', 2)

            # Update the corresponding batch job
            queued_req, queued_job = self._queue[job_id]
            queued_job._response = Response(
                method=queued_req.method,
                url=self._conn.url_prefix + queued_req.endpoint,
                headers={},
                status_code=int(status_code),
                status_text=status_text,
                raw_body=raw_body
            )
            queued_job._status = 'done'

        return self.jobs


class TransactionExecutor(DefaultExecutor):
    """Executes transaction API requests.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, API executions return instances of
        :class:`arango.job.TransactionJob`. These job instances are
        populated with results on commit. If set to False, API executions
        return None instead and no results are saved client-side.
    :type return_result: bool
    :param timeout: Collection lock timeout.
    :type timeout: int
    :param sync: Block until transaction is synchronized to disk.
    :type sync: bool
    :param read: Names of collections read during the transaction.
    :type read: [str | unicode]
    :param write: Names of collections written to during the transaction.
    :type write: [str | unicode]
    """
    context = 'transaction'

    def __init__(self, connection, return_result, read, write, timeout, sync):
        super(TransactionExecutor, self).__init__(connection)
        self._return_result = return_result
        self._read = read
        self._write = write
        self._timeout = timeout
        self._sync = sync
        self._queue = OrderedDict()
        self._committed = False

    def _verify_no_commit(self):
        """Verify that this transaction has not been committed already.

        :raise arango.exceptions.TransactionStateError: If already committed.
        """
        if self._committed:
            raise TransactionStateError('transaction already committed')

    @property
    def jobs(self):
        """Return the queued transaction jobs.

        :return: Transaction jobs or None if **return_result** parameter was
            set to False during initialization.
        :rtype: [arango.job.TransactionJob] | None
        """
        if not self._return_result:
            return None
        return [job for _, job in self._queue.values()]

    def execute(self, request, response_handler):
        """Put the request in the transaction queue.

        :param request: HTTP request.
        :type request: arango.request.Request
        :param response_handler: HTTP response handler.
        :type response_handler: callable
        :return: Transaction job or None if **return_result** parameter was
            set to False during initialization.
        :rtype: arango.job.TransactionJob | None
        :raise arango.exceptions.TransactionStateError: If the transaction was
            already committed or if the request is not supported.
        """
        self._verify_no_commit()
        if request.command is None:
            raise TransactionStateError('action not allowed in transaction')

        job = TransactionJob(response_handler)
        self._queue[job.id] = (request, job)
        return job if self._return_result else None

    def commit(self):
        """Execute the queued requests in a single transaction API request.

        If **return_result** parameter was set to True during initialization,
        :class:`arango.job.TransactionJob` instances are populated with
        results.

        :return: Transaction jobs or None if **return_result** parameter was
            set to False during initialization.
        :rtype: [arango.job.TransactionJob] | None
        :raise arango.exceptions.TransactionStateError: If the transaction was
            already committed.
        :raise arango.exceptions.TransactionExecuteError: If commit fails.
        """
        self._verify_no_commit()
        self._committed = True

        if len(self._queue) == 0:
            return self.jobs

        write_collections = set()
        read_collections = set()

        # Buffer for building the transaction javascript command
        cmd_buffer = [
            'var db = require("internal").db',
            'var gm = require("@arangodb/general-graph")',
            'var result = {}'
        ]
        for req, job in self._queue.values():
            if isinstance(req.read, string_types):
                read_collections.add(req.read)
            elif req.read is not None:
                read_collections |= set(req.read)

            if isinstance(req.write, string_types):
                write_collections.add(req.write)
            elif req.write is not None:
                write_collections |= set(req.write)

            cmd_buffer.append('result["{}"] = {}'.format(job.id, req.command))

        cmd_buffer.append('return result;')

        data = {
            'action': 'function () {{ {} }}'.format(';'.join(cmd_buffer)),
            'collections': {
                'read': list(read_collections),
                'write': list(write_collections),
                'allowImplicit': True
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
        for req, job in self._queue.values():
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
