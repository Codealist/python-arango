from __future__ import absolute_import, unicode_literals

from arango.utils import HTTP_OK
from arango.exceptions import (
    CursorNextError,
    CursorCloseError,
)
from arango import APIWrapper
from arango import Request
from arango.jobs import BaseJob


class Cursor(APIWrapper):
    """ArangoDB cursor which fetches documents from server in batches.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param init_data: The cursor initialization data.
    :type init_data: dict
    :raise CursorNextError: If the next batch of documents cannot be retrieved.
    :raise CursorCloseError: If the cursor cannot be closed.
    """
    type = 'cursor'

    def __init__(self, requester, init_data):
        super(Cursor, self).__init__(requester)
        self._data = init_data

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close(ignore_missing=True)

    def __repr__(self):
        if self.id is None:
            return '<ArangoDB cursor>'
        return '<ArangoDB cursor {}>'.format(self.id)

    @property
    def id(self):
        """Return the cursor ID.

        :return: The cursor ID.
        :rtype: str | unicode
        """
        return self._data.get('id')

    def batch(self):
        """Return the current batch of documents.

        :return: The current batch of documents.
        :rtype: list
        """
        return self._data['result']

    def has_more(self):
        """Return True if more results are available on the server.

        :return: True if more results are available.
        :rtype: bool
        """
        return self._data['hasMore']

    def count(self):
        """Return the total number of documents in the results.

        :return: The total number of documents, or None if the count option
            was not enabled during cursor initialization.
        :rtype: int
        """
        return self._data.get('count')

    def cached(self):
        """Return True if the results are cached.

        :return: True if the results are cached.
        :rtype: bool
        """
        return self._data.get('cached')

    def statistics(self):
        """Return cursor statistics.

        :return: The cursor statistics.
        :rtype: dict
        """
        if 'extra' in self._data and 'stats' in self._data['extra']:
            stats = dict(self._data['extra']['stats'])
            stats['modified'] = stats.pop('writesExecuted', None)
            stats['ignored'] = stats.pop('writesIgnored', None)
            stats['scanned_full'] = stats.pop('scannedFull', None)
            stats['scanned_index'] = stats.pop('scannedIndex', None)
            stats['execution_time'] = stats.pop('executionTime', None)
            return stats

    def warnings(self):
        """Return any warnings (from the query execution).

        :return: The warnings, or None if there are not any.
        :rtype: list
        """
        if 'extra' in self._data and 'warnings' in self._data['extra']:
            return self._data['extra']['warnings']

    def next(self):
        """Retrieve the next item in the cursor.

        :return: The next item in the cursor.
        :rtype: dict
        :raise StopIteration: If there is nothing left to return.
        :raise CursorNextError: If next item cannot be retrieved.
        """
        if len(self.batch()) == 0:
            if not self.has_more():
                raise StopIteration

            request = Request(
                method='put',
                endpoint='/_api/{}/{}'.format(self.type, self.id)
            )

            def response_handler(res):
                if res.status_code not in HTTP_OK:
                    raise CursorNextError(res)
                return res.body

            self._data = self._execute_request(request, response_handler)

        return self.batch().pop(0)

    def close(self, ignore_missing=True):
        """Close the cursor and free the resources tied to it.

        :param ignore_missing: Ignore missing cursors.
        :type ignore_missing: bool
        :return: True if the cursor was closed successfully.
        :rtype: bool
        :raise CursorCloseErrorL If the cursor cannot be closed.
        """
        if not self.id:
            return False

        request = Request(
            method='delete',
            endpoint='/_api/{}/{}'.format(self.type, self.id)
        )

        def response_handler(res):
            if res.status_code in HTTP_OK:
                return True
            if res.status_code == 404 and ignore_missing:
                return False
            raise CursorCloseError(res)

        return self._execute_request(request, response_handler)


class ExportCursor(Cursor):
    """ArangoDB cursor for export queries only."""
    type = 'export'
