from __future__ import absolute_import, unicode_literals

from arango import APIWrapper
from arango import Request
from arango.exceptions import (
    WALFlushError,
    WALPropertiesError,
    WALConfigureError,
    WALTransactionListError
)
from arango.utils import HTTP_OK


class WriteAheadLog(APIWrapper):
    """ArangoDB write-ahead log object.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    """

    def __init__(self, requester):
        super(WriteAheadLog, self).__init__(requester)

    def __repr__(self):
        return '<ArangoDB write-ahead log>'

    def properties(self):
        """Return the configuration of the write-ahead log.

        :return: The configuration of the write-ahead log
        :rtype: dict
        :raise arango.exceptions.WALPropertiesError: If request fails.
        """
        request = Request(
            method='get',
            endpoint='/_admin/wal/properties'
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise WALPropertiesError(res)
            return {
                'oversized_ops': res.body.get('allowOversizeEntries'),
                'log_size': res.body.get('logfileSize'),
                'historic_logs': res.body.get('historicLogfiles'),
                'reserve_logs': res.body.get('reserveLogfiles'),
                'sync_interval': res.body.get('syncInterval'),
                'throttle_wait': res.body.get('throttleWait'),
                'throttle_limit': res.body.get('throttleWhenPending')
            }

        return self._execute_request(request, response_handler)

    def configure(self,
                  oversized_ops=None,
                  log_size=None,
                  historic_logs=None,
                  reserve_logs=None,
                  throttle_wait=None,
                  throttle_limit=None):
        """Configure the write-ahead log.

        :param oversized_ops: execute and store ops bigger than a log file
        :type oversized_ops: bool
        :param log_size: The size of each write-ahead log file
        :type log_size: int
        :param historic_logs: The number of historic log files to keep
        :type historic_logs: int
        :param reserve_logs: The number of reserve log files to allocate
        :type reserve_logs: int
        :param throttle_wait: wait time before aborting when throttled (in ms)
        :type throttle_wait: int
        :param throttle_limit: number of pending gc ops before write-throttling
        :type throttle_limit: int
        :return: The new configuration of the write-ahead log
        :rtype: dict
        :raise arango.exceptions.WALPropertiesError: If request fails.
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

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise WALConfigureError(res)
            return {
                'oversized_ops': res.body.get('allowOversizeEntries'),
                'log_size': res.body.get('logfileSize'),
                'historic_logs': res.body.get('historicLogfiles'),
                'reserve_logs': res.body.get('reserveLogfiles'),
                'sync_interval': res.body.get('syncInterval'),
                'throttle_wait': res.body.get('throttleWait'),
                'throttle_limit': res.body.get('throttleWhenPending')
            }

        return self._execute_request(request, response_handler)

    def transactions(self):
        """Return details on currently running transactions.

        Fields in the returned in the result:

        .. code-block:: none

            "last_collected"    : The ID of the last collected log file (at
                                  the start of each running transaction) or
                                  None if no transactions are running.

            "last_sealed"       : The ID of the last sealed log file (at the
                                  start of each running transaction) or None
                                  if no transactions are running.

            "count"             : The number of current running transactions.

        :return: The details on currently running transactions.
        :rtype: dict
        :raise arango.exceptions.WALTransactionListError: If request fails.
        """

        request = Request(
            method='get',
            endpoint='/_admin/wal/transactions'
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise WALTransactionListError(res)
            return {
                'last_collected': res.body['minLastCollected'],
                'last_sealed': res.body['minLastSealed'],
                'count': res.body['runningTransactions']
            }

        return self._execute_request(request, response_handler)

    def flush(self, sync=True, garbage_collect=True):
        """Flush the write-ahead log to collection journals and data files.

        :param sync: If set to True, block until data is synced to disk.
        :type sync: bool
        :param garbage_collect: If set to True, block until flushed data is
            garbage collected.
        :type garbage_collect: bool
        :return: Whether the write-ahead log was flushed successfully.
        :rtype: bool
        :raise arango.exceptions.WALFlushError: If request fails.
        """
        request = Request(
            method='put',
            endpoint='/_admin/wal/flush',
            data={
                'waitForSync': sync,
                'waitForCollector': garbage_collect
            }
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise WALFlushError(res)
            return not res.body.get('error')

        return self._execute_request(request, response_handler)
