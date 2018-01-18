from __future__ import absolute_import, unicode_literals

from arango import APIWrapper
from arango import Request
from arango.cursor import Cursor, ExportCursor
from arango.exceptions import (
    CollectionBadStatusError,
    CollectionChecksumError,
    CollectionConfigureError,
    CollectionLoadError,
    CollectionPropertiesError,
    CollectionRenameError,
    CollectionRevisionError,
    CollectionRotateJournalError,
    CollectionStatisticsError,
    CollectionTruncateError,
    CollectionUnloadError,
    DocumentCountError,
    DocumentGetError,
    DocumentInError,
    IndexCreateError,
    IndexDeleteError,
    IndexListError,
    UserAccessError,
    UserRevokeAccessError,
    UserGrantAccessError
)
from arango.utils import HTTP_OK


class BaseCollection(APIWrapper):
    """Base for ArangoDB collection classes.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param name: The name of the collection.
    :type name: str | unicode
    """

    TYPES = {
        2: 'document',
        3: 'edge'
    }

    STATUSES = {
        1: 'new',
        2: 'unloaded',
        3: 'loaded',
        4: 'unloading',
        5: 'deleted',
        6: 'loading'
    }

    def __init__(self, requester, name):
        super(BaseCollection, self).__init__(requester)
        self._name = name

    def __iter__(self):
        """Iterate through the documents in the collection.

        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/all',
            data={'collection': self._name}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def __len__(self):
        """Return the number of documents in the collection.

        :return: The number of documents.
        :rtype: int
        :raise arango.exceptions.DocumentCountError: If the count fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/count'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentCountError(res)
            return res.body['count']

        return self._execute_request(request, response_handler)

    def __getitem__(self, key):
        """Return a document by its key from the collection.

        :param key: The document key.
        :type key: str | unicode
        :return: The document.
        :rtype: dict
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/document/{}/{}'.format(self._name, key)
        )

        def response_handler(res):
            if res.status_code in HTTP_OK:
                return res.body
            if res.status_code == 404 and res.error_code == 1202:
                return None
            raise DocumentGetError(res)

        return self._execute_request(request, response_handler)

    def __contains__(self, doc):
        """Check if a document exists in the collection by its key.

        :param doc: The document or its key.
        :type doc: dict | str | unicode
        :return: True if the document exists, False otherwise.
        :rtype: bool
        :raise arango.exceptions.DocumentInError: If the check fails.
        """
        return self.has(doc)

    def _status(self, code):
        """Return the collection status text.

        :param code: The collection status code.
        :type code: int
        :return: The collection status text or None.
        :rtype: str | unicode
        :raise arango.exceptions.CollectionBadStatusError: On unknown status.
        """
        if code is None:  # pragma: no cover
            return None
        try:
            return self.STATUSES[code]
        except KeyError:
            error_message = 'Unknown status code {}'.format(code)
            raise CollectionBadStatusError(error_message)

    @property
    def name(self):
        """Return the name of the collection.

        :return: The name of the collection
        :rtype: str | unicode
        """
        return self._name

    @property
    def database(self):
        """Return the name of the database the collection belongs to.

        :return: The name of the database.
        :rtype: str | unicode
        """
        return self._requester.database

    def rename(self, new_name):
        """Rename the collection.

        :param new_name: The new name for the collection.
        :type new_name: str | unicode
        :return: The new collection details.
        :rtype: dict
        :raise arango.exceptions.CollectionRenameError: If the rename fails.
        """
        # TODO disallow async or batch execution here
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rename'.format(self._name),
            data={'name': new_name}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRenameError(res)
            self._name = new_name
            return {
                'id': res.body['id'],
                'is_system': res.body['isSystem'],
                'name': res.body['name'],
                'status': self._status(res.body['status']),
                'type': self.TYPES[res.body['type']]
            }

        return self._execute_request(request, response_handler)

    def statistics(self):
        """Return the collection statistics.

        :return: The collection statistics.
        :rtype: dict
        :raise arango.exceptions.CollectionStatisticsError: If the retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/figures'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionStatisticsError(res)
            stats = res.body['figures']
            stats['compaction_status'] = stats.pop('compactionStatus', None)
            stats['document_refs'] = stats.pop('documentReferences', None)
            stats['last_tick'] = stats.pop('lastTick', None)
            stats['waiting_for'] = stats.pop('waitingFor', None)
            stats['uncollected_logfile_entries'] = stats.pop(
                'uncollectedLogfileEntries', None
            )
            return stats

        return self._execute_request(request, response_handler)

    def revision(self):
        """Return the collection revision.

        :return: The collection revision.
        :rtype: str | unicode
        :raise arango.exceptions.CollectionRevisionError: If the retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/revision'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRevisionError(res)
            return res.body['revision']

        return self._execute_request(request, response_handler)

    def properties(self):
        """Return the collection properties.

        :return: The collection properties.
        :rtype: dict
        :raise arango.exceptions.CollectionPropertiesError: If the retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/properties'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionPropertiesError(res)

            key_options = res.body.get('keyOptions', {})

            return {
                'id': res.body.get('id'),
                'name': res.body.get('name'),
                'edge': res.body.get('type') == 3,
                'sync': res.body.get('waitForSync'),
                'status': self._status(res.body.get('status')),
                'compact': res.body.get('doCompact'),
                'system': res.body.get('isSystem'),
                'volatile': res.body.get('isVolatile'),
                'journal_size': res.body.get('journalSize'),
                'keygen': key_options.get('type'),
                'user_keys': key_options.get('allowUserKeys'),
                'key_increment': key_options.get('increment'),
                'key_offset': key_options.get('offset')
            }

        return self._execute_request(request, response_handler)

    def configure(self, sync=None, journal_size=None):
        """Configure the collection properties.

        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param journal_size: The journal size.
        :type journal_size: int
        :return: The new collection properties.
        :rtype: dict
        :raise arango.exceptions.CollectionConfigureError: If the configure
            operation fails.

        .. note::
            Only *sync* and *journal_size* are configurable properties.
        """
        data = {}
        if sync is not None:
            data['waitForSync'] = sync
        if journal_size is not None:
            data['journalSize'] = journal_size

        request = Request(
            method='put',
            endpoint='/_api/collection/{}/properties'.format(self._name),
            data=data
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionConfigureError(res)

            key_options = res.body.get('keyOptions', {})

            return {
                'id': res.body.get('id'),
                'name': res.body.get('name'),
                'edge': res.body.get('type') == 3,
                'sync': res.body.get('waitForSync'),
                'status': self._status(res.body.get('status')),
                'compact': res.body.get('doCompact'),
                'system': res.body.get('isSystem'),
                'volatile': res.body.get('isVolatile'),
                'journal_size': res.body.get('journalSize'),
                'keygen': key_options.get('type'),
                'user_keys': key_options.get('allowUserKeys'),
                'key_increment': key_options.get('increment'),
                'key_offset': key_options.get('offset')
            }

        return self._execute_request(request, response_handler)

    def load(self):
        """Load the collection into memory.

        :return: The collection status.
        :rtype: str | unicode
        :raise arango.exceptions.CollectionLoadError: If the load fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/load'.format(self._name),
            command='db.{}.load()'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionLoadError(res)
            return self._status(res.body['status'])

        return self._execute_request(request, response_handler)

    def unload(self):
        """Unload the collection from memory.

        :return: The collection status.
        :rtype: str | unicode
        :raise arango.exceptions.CollectionUnloadError: If the unload fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/unload'.format(self._name),
            command='db.{}.unload()'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionUnloadError(res)
            return self._status(res.body['status'])

        return self._execute_request(request, response_handler)

    def rotate(self):
        """Rotate the collection journal.

        :return: The result of the rotate operation.
        :rtype: dict
        :raise arango.exceptions.CollectionRotateJournalError: If the rotate
            operation fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rotate'.format(self._name),
            command='db.{}.rotate()'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRotateJournalError(res)
            return res.body['result']  # pragma: no cover

        return self._execute_request(request, response_handler)

    def checksum(self, with_rev=False, with_data=False):
        """Return the collection checksum.

        :param with_rev: Include document revisions in checksum calculations.
        :type with_rev: bool
        :param with_data: Include document data in checksum calculations.
        :type with_data: bool
        :return: The collection checksum.
        :rtype: int
        :raise arango.exceptions.CollectionChecksumError: If the checksum
            operation fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/checksum'.format(self._name),
            params={'withRevision': with_rev, 'withData': with_data}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionChecksumError(res)
            return int(res.body['checksum'])

        return self._execute_request(request, response_handler)

    def truncate(self):
        """Delete all documents in the collection.

        :return: The collection details.
        :rtype: dict
        :raise arango.exceptions.CollectionTruncateError: If the delete fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/truncate'.format(self._name),
            command='db.{}.truncate()'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionTruncateError(res)
            return {
                'id': res.body['id'],
                'is_system': res.body['isSystem'],
                'name': res.body['name'],
                'status': self._status(res.body['status']),
                'type': self.TYPES[res.body['type']]
            }

        return self._execute_request(request, response_handler)

    def count(self):
        """Return the number of documents in the collection.

        :return: The number of documents.
        :rtype: int
        :raise arango.exceptions.DocumentCountError: If the retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/count'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentCountError(res)
            return res.body['count']

        return self._execute_request(request, response_handler)

    def has(self, document, rev=None, match_rev=True):
        """Check if a document exists in the collection by its key.

        :param document: The document or its key.
        :type document: dict | str | unicode
        :param rev: The expected document revision.
        :type rev: str | unicode
        :param match_rev: This parameter applies only when **rev** is given. If
            set to True, ensure that the document revision matches the value of
            **rev**. Otherwise, ensure that they do not match.
        :type match_rev: bool
        :return: True if the document exists, False otherwise.
        :rtype: bool
        :raise arango.exceptions.DocumentRevisionError: If **rev** is given and
            it does not match the target document revision.
        :raise arango.exceptions.DocumentInError: If the check fails.
        """
        headers = {}
        if rev is not None:
            if match_rev:
                headers['If-Match'] = rev
            else:
                headers['If-None-Match'] = rev

        key = document['_key'] if isinstance(document, dict) else document

        request = Request(
            method='get',
            endpoint='/_api/document/{}/{}'.format(self._name, key),
            headers=headers
        )

        def response_handler(res):
            if res.status_code == 404 and res.error_code == 1202:
                return False
            elif res.status_code in HTTP_OK:
                return True
            raise DocumentInError(res)

        return self._execute_request(request, response_handler)

    def all(self, skip=None, limit=None):
        """Return all documents in the collection using a server cursor.

        :param skip: The number of documents to skip.
        :type skip: int
        :param limit: The max number of documents fetched by the cursor.
        :type limit: int
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        data = {'collection': self._name}
        if skip is not None:
            data['skip'] = skip
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/all',
            data=data
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def export(self,
               limit=None,
               count=False,
               batch_size=None,
               flush=None,
               flush_wait=None,
               ttl=None,
               filter_fields=None,
               filter_type='include'):  # pragma: no cover
        """"Export all documents in the collection using a server cursor.

        :param flush: Flush the WAL prior to the export.
        :type flush: bool
        :param flush_wait: The max wait time in seconds for the WAL flush.
        :type flush_wait: int
        :param count: Include the document count in the server cursor.
        :type count: bool
        :param batch_size: The max number of documents in the batch fetched by
            the cursor in one round trip.
        :type batch_size: int
        :param limit: The max number of documents fetched by the cursor.
        :type limit: int
        :param ttl: The time-to-live for the cursor on the server.
        :type ttl: int
        :param filter_fields: Fields used to filter documents.
        :type filter_fields: [str | unicode]
        :param filter_type: Allowed values are "include" or "exclude".
        :type filter_type: str | unicode
        :return: The document export cursor.
        :rtype: arango.cursor.ExportCursor
        :raise arango.exceptions.DocumentGetError: If the export fails.

        .. note::
            If **flush** is not set to True, the documents in WAL during
            export are *not* included by the server cursor.
        """
        data = {'count': count}
        if flush is not None:  # pragma: no cover
            data['flush'] = flush
        if flush_wait is not None:  # pragma: no cover
            data['flushWait'] = flush_wait
        if batch_size is not None:
            data['batchSize'] = batch_size
        if limit is not None:
            data['limit'] = limit
        if ttl is not None:
            data['ttl'] = ttl
        if filter_fields is not None:
            data['restrict'] = {
                'fields': filter_fields,
                'type': filter_type
            }
        request = Request(
            method='post',
            endpoint='/_api/export',
            params={'collection': self._name},
            data=data
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return ExportCursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def find(self, filters, offset=None, limit=None):
        """Return all documents that match the given filters.

        :param filters: The document filters.
        :type filters: dict
        :param offset: The number of documents to skip.
        :type offset: int
        :param limit: The max number of documents to return.
        :type limit: int
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        data = {'collection': self._name, 'example': filters}
        if offset is not None:
            data['skip'] = offset
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/by-example',
            data=data
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def get_many(self, keys):
        """Return multiple documents by their keys.

        :param keys: The list of document keys.
        :type keys: [str | unicode]
        :return: The documents.
        :rtype: [dict]
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/lookup-by-keys',
            data={'collection': self._name, 'keys': keys}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return res.body['documents']

        return self._execute_request(request, response_handler)

    def random(self):
        """Return a random document from the collection.

        :return: A random document.
        :rtype: dict
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/any',
            data={'collection': self._name}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return res.body['document']

        return self._execute_request(request, response_handler)

    def find_near(self, latitude, longitude, limit=None):
        """Return documents near a given coordinate.

        By default, at most 100 documents near the coordinate are returned.
        Documents returned are sorted according to distance, with the nearest
        document being the first. If there are documents of equal distance,
        they are be randomly chosen from the set until the limit is reached.

        :param latitude: The latitude.
        :type latitude: int
        :param longitude: The longitude.
        :type longitude: int
        :param limit: The max number of documents to return.
        :type limit: int
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.

        .. note::
            A geo index is required to use this method.
        """

        if limit is None:
            limit_string = ''
        else:
            limit_string = ', @limit'

        full_query = """
        FOR doc IN NEAR(@collection, @latitude, @longitude{})
            RETURN doc
        """.format(limit_string)

        bind_vars = {
            'collection': self._name,
            'latitude': latitude,
            'longitude': longitude
        }
        if limit is not None:
            bind_vars['limit'] = limit

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def find_in_range(self,
                      field,
                      lower,
                      upper,
                      offset=0,
                      limit=100,
                      inclusive=True):
        """Return documents within a given range in a random order.

        :param field: The document field to use.
        :type field: str | unicode
        :param lower: The lower bound.
        :type lower: int
        :param upper: The upper bound.
        :type upper: int
        :param offset: The number of documents to skip.
        :type offset: int
        :param limit: The max number of documents to return.
        :type limit: int
        :param inclusive: Include the lower and upper bounds.
        :type inclusive: bool
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        if inclusive:
            full_query = """
            FOR doc IN @@collection
                FILTER doc.@field >= @lower && doc.@field <= @upper
                LIMIT @skip, @limit
                RETURN doc
            """
        else:
            full_query = """
            FOR doc IN @@collection
                FILTER doc.@field > @lower && doc.@field < @upper
                LIMIT @skip, @limit
                RETURN doc
            """
        bind_vars = {
            '@collection': self._name,
            'field': field,
            'lower': lower,
            'upper': upper,
            'skip': offset,
            'limit': limit
        }

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    # TODO the WITHIN geo function does not seem to work properly
    def find_in_radius(self, latitude, longitude, radius, distance_field=None):
        """Return documents within a given radius in a random order.

        :param latitude: The latitude.
        :type latitude: int
        :param longitude: The longitude.
        :type longitude: int
        :param radius: The maximum radius.
        :type radius: int
        :param distance_field: The document field containing the distance.
        :type distance_field: str | unicode
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.

        .. note::
            A geo index is required to use this method.
        """

        if distance_field:
            distance_string = ', @distance'
        else:
            distance_string = ''

        full_query = """
        FOR doc IN WITHIN(@collection, @latitude, @longitude, @radius{})
            RETURN doc
        """.format(distance_string)

        bind_vars = {
            'collection': self._name,
            'latitude': latitude,
            'longitude': longitude,
            'radius': radius
        }
        if distance_field is not None:
            bind_vars['distance'] = distance_field

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def find_in_box(self,
                    latitude1,
                    longitude1,
                    latitude2,
                    longitude2,
                    skip=None,
                    limit=None,
                    geo_field=None):
        """Return all documents in an rectangular area.

        :param latitude1: The first latitude.
        :type latitude1: int
        :param longitude1: The first longitude.
        :type longitude1: int
        :param latitude2: The second latitude.
        :type latitude2: int
        :param longitude2: The second longitude.
        :type longitude2: int
        :param skip: The number of documents to skip.
        :type skip: int
        :param limit: The max number of documents to return. If set to 0, all
            documents are returned.
        :type limit: int
        :param geo_field: The field with the geo index.
        :type geo_field: str | unicode
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        data = {
            'collection': self._name,
            'latitude1': latitude1,
            'longitude1': longitude1,
            'latitude2': latitude2,
            'longitude2': longitude2,
        }
        if skip is not None:
            data['skip'] = skip
        if limit is not None:
            data['limit'] = limit
        if geo_field is not None:
            data['geo'] = '/'.join([self._name, geo_field])

        request = Request(
            method='put',
            endpoint='/_api/simple/within-rectangle',
            data=data
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    # TODO this is broken in 3.3
    def find_by_text(self, field, query, limit=None):
        """Return documents that match the specified fulltext **query**.

        :param field: The document field with the fulltext index.
        :type field: str | unicode
        :param query: The fulltext query.
        :type query: str | unicode
        :param limit: The max number of documents to return.
        :type limit: int
        :return: The document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        if limit:
            limit_string = ', @limit'
        else:
            limit_string = ''

        full_query = """
        FOR doc IN FULLTEXT(@collection, @field, @query{})
            RETURN doc
        """.format(limit_string)

        bind_vars = {
            'collection': self._name,
            'field': field,
            'query': query
        }
        if limit is not None:
            bind_vars['limit'] = limit

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return Cursor(self._requester, res.body)

        return self._execute_request(request, response_handler)

    def indexes(self):
        """Return the collection indexes.

        :return: The collection indexes.
        :rtype: [dict]
        :raise arango.exceptions.IndexListError: If the retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/index',
            params={'collection': self._name}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexListError(res)

            indexes = []
            for index in res.body['indexes']:
                index['id'] = index['id'].split('/', 1)[1]
                if 'minLength' in index:
                    index['min_length'] = index.pop('minLength')
                if 'geoJson' in index:
                    index['geo_json'] = index.pop('geoJson')
                if 'ignoreNull' in index:
                    index['ignore_none'] = index.pop('ignoreNull')
                if 'selectivityEstimate' in index:
                    index['selectivity'] = index.pop('selectivityEstimate')
                indexes.append(index)
            return indexes

        return self._execute_request(request, response_handler)

    def _add_index(self, data):
        """Helper method for creating a new index."""
        request = Request(
            method='post',
            endpoint='/_api/index',
            data=data,
            params={'collection': self._name}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexCreateError(res)
            details = res.body
            details['id'] = details['id'].split('/', 1)[1]
            details.pop('error', None)
            details.pop('code', None)
            if 'minLength' in details:
                details['min_length'] = details.pop('minLength')
            if 'geoJson' in details:
                details['geo_json'] = details.pop('geoJson')
            if 'ignoreNull' in details:
                details['ignore_none'] = details.pop('ignoreNull')
            if 'selectivityEstimate' in details:
                details['selectivity'] = details.pop('selectivityEstimate')
            if 'isNewlyCreated' in details:
                details['new'] = details.pop('isNewlyCreated')
            return details

        return self._execute_request(request, response_handler)

    def add_hash_index(self,
                       fields,
                       unique=None,
                       sparse=None,
                       deduplicate=None):
        """Create a new hash index.

        :param fields: The document fields to index.
        :type fields: [str | unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: If set to True, documents with None in the field
            are also indexed. Otherwise they are skipped.
        :type sparse: bool
        :param deduplicate: Whether inserting duplicate index values from the
            same document triggers unique constraint errors or not.
        :param deduplicate: bool
        :return: The details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If the create fails.
        """
        data = {'type': 'hash', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        if deduplicate is not None:
            data['deduplicate'] = deduplicate
        return self._add_index(data)

    def add_skiplist_index(self,
                           fields,
                           unique=None,
                           sparse=None,
                           deduplicate=None):
        """Create a new skiplist index.

        :param fields: The document fields to index.
        :type fields: [str | unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: If set to True, documents with None in the field
            are also indexed. Otherwise they are skipped.
        :type sparse: bool
        :param deduplicate: Whether inserting duplicate index values from the
            same document triggers unique constraint errors or not.
        :param deduplicate: bool
        :return: The details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If the create fails.
        """
        data = {'type': 'skiplist', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        if deduplicate is not None:
            data['deduplicate'] = deduplicate
        return self._add_index(data)

    def add_geo_index(self, fields, ordered=None):
        """Create a geo-spatial index.

        :param fields: A single document field or a list of document fields. If
            a single field is given, the field must have values that ar lists
            with at least two floats. Documents with missing fields or invalid
            values are excluded.
        :type fields: str | unicode | [str | unicode]
        :param ordered: Whether the order is longitude then latitude.
        :type ordered: bool
        :return: The details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If the create fails.
        """
        data = {'type': 'geo', 'fields': fields}
        if ordered is not None:
            data['geoJson'] = ordered
        return self._add_index(data)

    def add_fulltext_index(self, fields, min_length=None):
        """Create a fulltext index.

        :param fields: The document fields to index.
        :type fields: [str | unicode]
        :param min_length: The minimum number of characters to index.
        :type min_length: int
        :return: The details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If the create fails.
        """
        # TODO keep an eye on this for future ArangoDB releases
        if len(fields) > 1:
            raise IndexCreateError('Only one field is currently supported')

        data = {'type': 'fulltext', 'fields': fields}
        if min_length is not None:
            data['minLength'] = min_length
        return self._add_index(data)

    def add_persistent_index(self, fields, unique=None, sparse=None):
        """Create a persistent index.

        :param fields: The document fields to index.
        :type fields: [str | unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: Exclude documents that do not contain at least one of
            the indexed fields, or documents that have a value of None in
            any of the indexed fields.
        :type sparse: bool
        :return: The details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If the create fails.

        .. note::
            Unique persistent indexes on non-sharded keys are not supported
            in a cluster.
        """
        data = {'type': 'persistent', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        return self._add_index(data)

    def delete_index(self, index_id, ignore_missing=False):
        """Delete an index.

        :param index_id: The ID of the index to delete.
        :type index_id: str | unicode
        :param ignore_missing: Do not raise an exception on missing indexes.
        :type ignore_missing: bool
        :return: True if deleted successfully, False otherwise.
        :rtype: bool
        :raise arango.exceptions.IndexDeleteError: If the delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/index/{}/{}'.format(self._name, index_id)
        )

        def response_handler(res):
            if res.status_code == 404 and res.error_code == 1212:
                if ignore_missing:
                    return False
                raise IndexDeleteError(res)
            if res.status_code not in HTTP_OK:
                raise IndexDeleteError(res)
            return not res.body['error']

        return self._execute_request(request, response_handler)

    def user_access(self, username):
        """Return a user's access details.

        :param username: The name of the user.
        :type username: str | unicode
        :return: The access details (e.g. "rw", None)
        :rtype: str | unicode
        :raise: arango.exceptions.UserAccessError: If the retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/user/{}/database/{}/{}'.format(
                username, self.database, self.name
            )
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise UserAccessError(res)
            result = res.body['result'].lower()
            return None if result == 'none' else result

        return self._execute_request(request, response_handler)

    def grant_user_access(self, username):
        """Grant user access to the collection.

        :param username: The name of the user.
        :type username: str | unicode
        :return: True if successful, False otherwise.
        :rtype: bool
        :raise arango.exceptions.UserGrantAccessError: If the operation fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/user/{}/database/{}/{}'.format(
                username, self.database, self.name
            ),
            data={'grant': 'rw'}
        )

        def response_handler(res):
            if res.status_code in HTTP_OK:
                return True
            raise UserGrantAccessError(res)

        return self._execute_request(request, response_handler)

    def revoke_user_access(self, username):
        """Revoke user access to the collection.

        :param username: The name of the user.
        :type username: str | unicode
        :return: True if successful, False otherwise.
        :rtype: bool
        :raise arango.exceptions.UserRevokeAccessError: If the operation fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/user/{}/database/{}/{}'.format(
                username, self.database, self.name
            )
        )

        def response_handler(res):
            if res.status_code in HTTP_OK:
                return True
            raise UserRevokeAccessError(res)

        return self._execute_request(request, response_handler)
