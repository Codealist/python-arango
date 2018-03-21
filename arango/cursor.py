from __future__ import absolute_import, unicode_literals

from arango.exceptions import (
    CursorNextError,
    CursorCloseError,
)
from arango.request import Request
from arango.utils import is_list


class Cursor(object):
    """ArangoDB cursor which fetches documents from server in batches.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param init_data: Cursor initialization data.
    :type init_data: dict
    :param cursor_type: Cursor type. Allowed values are "cursor" for standard
        cursors and "export" for export cursors.
    :type cursor_type: str or unicode
    """

    def __init__(self, connection, init_data, cursor_type='cursor'):
        self._conn = connection
        self._type = cursor_type
        self._id = None
        self._count = None
        self._cached = None
        self._batch = None
        self._has_more = None
        self._stats = None
        self._profile = None
        self._warnings = None
        self._update_data(init_data)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __enter__(self):
        return self

    def __len__(self):
        return self._count

    def __exit__(self, *_):
        self.close(ignore_missing=True)

    def __repr__(self):
        return '<Cursor {}>'.format(self._id)

    def _update_data(self, data):
        """Update the cursor using the data from ArangoDB.

        :param data: Cursor data from ArangoDB.
        :type data: dict
        """
        if is_list(data):
            self._count = len(data)
            self._has_more = False
            self._batch = data
        else:
            if self._id is None:
                self._id = data.get('id')
            self._count = data.get('count')
            self._cached = data.get('cached')
            self._has_more = data['hasMore']
            self._batch = data['result']

            if 'extra' in data:
                extra = data['extra']

                if 'profile' in extra:
                    self._profile = extra['profile']

                if 'warnings' in extra:
                    self._warnings = extra['warnings']

                if 'stats' in extra:
                    stats = extra['stats']
                    if 'writesExecuted' in stats:
                        stats['modified'] = stats.pop('writesExecuted')
                    if 'writesIgnored' in stats:
                        stats['ignored'] = stats.pop('writesIgnored')
                    if 'scannedFull' in stats:
                        stats['scanned_full'] = stats.pop('scannedFull')
                    if 'scannedIndex' in stats:
                        stats['scanned_index'] = stats.pop('scannedIndex')
                    if 'executionTime' in stats:
                        stats['execution_time'] = stats.pop('executionTime')
                    if 'httpRequests' in stats:
                        stats['http_requests'] = stats.pop('httpRequests')
                    self._stats = stats

    @property
    def id(self):
        """Return the cursor ID.

        :return: Cursor ID.
        :rtype: str or unicode
        """
        return self._id

    @property
    def type(self):
        """Return the cursor type.

        :return: Cursor type ("cursor" or "export").
        :rtype: str or unicode.
        """
        return self._type

    @property
    def batch(self):
        """Return the current batch of documents.

        :return: Current batch of documents.
        :rtype: list
        """
        return self._batch

    @property
    def has_more(self):
        """Return True if more results are available on the server.

        :return: True if more results are available.
        :rtype: bool
        """
        return self._has_more

    @property
    def count(self):
        """Return the total number of documents in the results.

        :return: Total number of documents, or None if the count option
            was not enabled during cursor initialization.
        :rtype: int
        """
        return self._count

    @property
    def cached(self):
        """Return True if the results are cached.

        :return: True if the results are cached.
        :rtype: bool
        """
        return self._cached

    @property
    def statistics(self):
        """Return cursor statistics.

        :return: Cursor statistics.
        :rtype: dict
        """
        return self._stats

    @property
    def profile(self):
        """Return cursor performance profile.

        :return: Cursor performance profile.
        :rtype: dict
        """
        return self._profile

    @property
    def warnings(self):
        """Return any warnings (from the query execution).

        :return: Warnings, or None if there are none.
        :rtype: list
        """
        return self._warnings

    def next(self):
        """Retrieve the next item in the cursor.

        :return: Next item in the cursor.
        :rtype: dict
        :raise StopIteration: If there is nothing left to return.
        :raise CursorNextError: If next item cannot be retrieved.
        """
        if len(self._batch) == 0:
            if not self._has_more:
                raise StopIteration

            request = Request(
                method='put',
                endpoint='/_api/{}/{}'.format(self._type, self.id)
            )
            resp = self._conn.send_request(request)

            if not resp.is_success:
                raise CursorNextError(resp)
            self._update_data(resp.body)

        return self._batch.pop(0)

    def close(self, ignore_missing=True):
        """Close the cursor and free the resources tied to it.

        :param ignore_missing: Do not raise exception on missing cursors.
        :type ignore_missing: bool
        :return: True if the cursor was closed successfully.
        :rtype: bool
        :raise CursorCloseError: If the cursor cannot be closed.
        """
        if self.id is None:
            return False
        request = Request(
            method='delete',
            endpoint='/_api/{}/{}'.format(self._type, self.id)
        )
        resp = self._conn.send_request(request)
        if resp.is_success:
            return True
        if resp.status_code == 404 and ignore_missing:
            return False
        raise CursorCloseError(resp)
