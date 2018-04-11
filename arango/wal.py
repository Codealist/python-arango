from __future__ import absolute_import, unicode_literals

__all__ = ['WAL']

from arango.api import APIWrapper
from arango.exceptions import (
    WALFlushError,
    WALPropertiesError,
    WALConfigureError,
    WALTransactionListError
)
from arango.request import Request


class WAL(APIWrapper):
    """ArangoDB write-ahead log.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.executor.DefaultExecutor
    """

    def __init__(self, connection, executor):
        super(WAL, self).__init__(connection, executor)

    # noinspection PyMethodMayBeStatic
    def _format_properties(self, body):
        if 'allowOversizeEntries' in body:
            body['oversized_ops'] = body.pop('allowOversizeEntries')
        if 'logfileSize' in body:
            body['log_size'] = body.pop('logfileSize')
        if 'historicLogfiles' in body:
            body['historic_logs'] = body.pop('historicLogfiles')
        if 'reserveLogfiles' in body:
            body['reserve_logs'] = body.pop('reserveLogfiles')
        if 'syncInterval' in body:
            body['sync_interval'] = body.pop('syncInterval')
        if 'throttleWait' in body:
            body['throttle_wait'] = body.pop('throttleWait')
        if 'throttleWhenPending' in body:
            body['throttle_limit'] = body.pop('throttleWhenPending')
        return body

    def properties(self):
        """Return the configuration of the write-ahead log.

        :return: Write-ahead log configuration.
        :rtype: dict
        :raise arango.exceptions.WALPropertiesError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_admin/wal/properties'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise WALPropertiesError(resp)
            return self._format_properties(resp.body)

        return self._execute(request, response_handler)

    def configure(self,
                  oversized_ops=None,
                  log_size=None,
                  historic_logs=None,
                  reserve_logs=None,
                  throttle_wait=None,
                  throttle_limit=None):
        """Set write-ahead log properties.

        :param oversized_ops: Execute and store ops bigger than a log file.
        :type oversized_ops: bool
        :param log_size: Size of each write-ahead log file
        :type log_size: int
        :param historic_logs: Number of historic log files to keep.
        :type historic_logs: int
        :param reserve_logs: Number of reserve log files to allocate.
        :type reserve_logs: int
        :param throttle_wait: Wait time before aborting when throttled in ms.
        :type throttle_wait: int
        :param throttle_limit: Number of pending garbage collector operations
            before write-throttling.
        :type throttle_limit: int
        :return: New configuration of the write-ahead log.
        :rtype: dict
        :raise arango.exceptions.WALConfigureError: If configuration fails.
        """
        data = {}
        if oversized_ops is not None:
            data['allowOversizeEntries'] = oversized_ops
        if log_size is not None:
            data['logfileSize'] = log_size
        if historic_logs is not None:
            data['historicLogfiles'] = historic_logs
        if reserve_logs is not None:
            data['reserveLogfiles'] = reserve_logs
        if throttle_wait is not None:
            data['throttleWait'] = throttle_wait
        if throttle_limit is not None:
            data['throttleWhenPending'] = throttle_limit

        request = Request(
            method='put',
            endpoint='/_admin/wal/properties',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise WALConfigureError(resp)
            return self._format_properties(resp.body)

        return self._execute(request, response_handler)

    def transactions(self):
        """Return details on currently running transactions.

        Fields in the returned in the result:

        .. code-block:: none

            "last_collected"    : ID of the last collected log file (at the
                                  start of each running transaction) or None
                                  if no transactions are running.

            "last_sealed"       : ID of the last sealed log file (at the start
                                  of each running transaction) or None if no
                                  transactions are running.

            "count"             : Number of current running transactions.

        :return: Details on currently running transactions.
        :rtype: dict
        :raise arango.exceptions.WALTransactionListError: If retrieval fails.
        """

        request = Request(
            method='get',
            endpoint='/_admin/wal/transactions'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise WALTransactionListError(resp)
            if 'minLastCollected' in resp.body:
                resp.body['last_collected'] = resp.body.pop('minLastCollected')
            if 'minLastSealed' in resp.body:
                resp.body['last_sealed'] = resp.body.pop('minLastSealed')
            if 'runningTransactions' in resp.body:
                resp.body['count'] = resp.body.pop('runningTransactions')
            return resp.body

        return self._execute(request, response_handler)

    def flush(self, sync=True, garbage_collect=True):
        """Flush the write-ahead log to collection journals and data files.

        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param garbage_collect: Block until flushed data is garbage collected.
        :type garbage_collect: bool
        :return: Whether the write-ahead log was flushed successfully.
        :rtype: bool
        :raise arango.exceptions.WALFlushError: If flush fails.
        """
        request = Request(
            method='put',
            endpoint='/_admin/wal/flush',
            data={
                'waitForSync': sync,
                'waitForCollector': garbage_collect
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise WALFlushError(resp)
            return True

        return self._execute(request, response_handler)
