from __future__ import absolute_import, unicode_literals

from collections import OrderedDict
from uuid import uuid4

import arango.database
from arango.exceptions import (
    BatchBadStateError,
    BatchExecuteError,
    BatchJobResultError
)
from arango.api import APIExecutor
from arango.request import Request
from arango.response import Response
from arango.utils import suppress_warning


class BatchExecutor(APIExecutor):
    """Executes batch API requests.

    :param batch: Batch object.
    :type batch: arango.batch.Batch
    """

    def __init__(self, batch):
        self._batch = batch

    # noinspection PyProtectedMember
    def execute(self, _, request, response_handler):
        return self._batch._add_request(request, response_handler)


class Batch(object):
    """Batch object which keeps track of the execution state.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, API requests are queued client-side
        and instances :class:`arango.batch.BatchJob` are returned to user. Job
        instances are populated with the results on commit. If set to False,
        requests are queued and executed but results are not saved server-side.
        Job objects are not returned to the user.
    :type return_result: bool
    """

    def __init__(self, connection, return_result=True):
        self._id = uuid4().hex
        self._status = 'pending'
        self._conn = connection
        self._return_result = return_result
        self._queue = OrderedDict()
        self._executor = BatchExecutor(self)
        self._database = arango.database.Database(self._conn, self._executor)

    def __repr__(self):
        return '<Batch {}>'.format(self._id)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None:
            self.commit()

    def _verify_no_commit(self):
        if self._status == 'done':
            raise BatchBadStateError(
                message='batch {} committed already.'.format(self._id))

    def _add_request(self, request, response_handler):
        self._verify_no_commit()
        job = BatchJob(response_handler)
        self._queue[job.id] = (request, job)
        return job if self._return_result else None

    @property
    def id(self):
        """Return the batch ID.

        :return: Batch ID.
        :rtype: str | unicode
        """
        return self._id

    @property
    def status(self):
        """Return the batch status.

        If the batch is not committed, the status is set to "pending". If it
        is committed, the status is changed to "done".

        :return: Batch status.
        :rtype: str | unicode
        """
        return self._status

    @property
    def db(self):
        """Return the database wrapper for batch execution.

        :return: Database wrapper.
        :rtype: arango.database.Database
        """
        return self._database

    @property
    def jobs(self):
        """Return the jobs in this batch instance.

        :return: Batch jobs.
        :rtype: [arango.batch.BatchJob]
        """
        if not self._return_result:
            return None
        return [job for _, job in self._queue.values()]

    # noinspection PyProtectedMember
    def commit(self):
        """Execute the queued requests in a single API call.

        If **return_result** was set to True during the initialization of the
        executor, the :class:`arango.batch.BatchJob` instances returne are
        automatically populated with the results.

        :return: List of batch jobs.
        :rtype: [arango.batch.BatchJob]

        :raise arango.exceptions.BatchExecuteError: If commit fails.
        """
        self._verify_no_commit()
        self._status = 'done'
        if len(self._queue) == 0:
            return self.jobs

        # Boundary used for multipart request
        boundary = uuid4().hex

        # Buffer for building the payload
        buffer = []

        # Build the batch request payload from the queued jobs
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
            raise BatchBadStateError(
                message='expecting {} parts in batch response but got {}'
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


class BatchJob(object):
    """Batch API call job.

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
        """Return the job ID.

        :return: Job ID.
        :rtype: str or unicode
        """
        return self._id

    @property
    def status(self):
        """Return the batch job status.

        :return: Batch job status.
        :rtype: str or unicode
        """
        return self._status

    def result(self, raise_errors=False):
        """Return the result of the batch job is available.

        :param raise_errors: If set to True, any exception raised during the
            job execution is propagated up. If set to False, the exception is
            not raised but returned as an object.
        :type raise_errors: bool
        :return: Batch job result.
        :rtype: object
        :raise arango.exceptions.BatchJobResultError: If result is not
            available. For example, the batch was not committed yet.
        :raise arango.exceptions.ArangoError: If **raise_errors* was set to
            True and the execution failed, the exception is propagated up.
        """
        if self._status == 'pending':
            raise BatchJobResultError(message='result not available yet')
        try:
            result = self._response_handler(self._response)
        except Exception as error:
            if raise_errors:
                raise
            return error
        else:
            return result
