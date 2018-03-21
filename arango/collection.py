from __future__ import absolute_import, unicode_literals

from json import dumps

__all__ = ['Collection', 'VertexCollection', 'EdgeCollection']

from arango.api import APIWrapper
from arango.cursor import Cursor
from arango.exceptions import (
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
    DocumentInError,
    DocumentDeleteError,
    DocumentGetError,
    DocumentKeysError,
    DocumentIDsError,
    DocumentInsertError,
    DocumentParseError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    IndexCreateError,
    IndexDeleteError,
    IndexListError,
)
from arango.request import Request
from arango.response import Response
from arango.utils import is_list, is_str, is_int, is_dict


class Base(APIWrapper):
    """Base for ArangoDB collection classes.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    :param name: Collection name.
    :type name: str or unicode
    """

    types = {
        2: 'document',
        3: 'edge'
    }

    statuses = {
        1: 'new',
        2: 'unloaded',
        3: 'loaded',
        4: 'unloading',
        5: 'deleted',
        6: 'loading'
    }

    def __init__(self, connection, executor, name):
        super(Base, self).__init__(connection, executor)
        self._name = name

    def __iter__(self):
        return self.all()

    def __len__(self):
        return self.count()

    def __contains__(self, document):
        return self.has(document, check_rev=True)

    def _get_status_string(self, code):
        """Return the collection status text.

        :param code: Collection status code.
        :type code: int
        :return: Collection status text or None.
        :rtype: str or unicode
        :raise arango.exceptions.CollectionBadStatusError: On unknown status.
        """
        return code if code is None else self.statuses[code]

    def _format_properties(self, body):
        """Format the collection properties.

        :param body: Response body.
        :type body: dict
        :return: Formatted body.
        :rtype: dict
        """
        if 'code' in body:
            body.pop('code')
        if 'error' in body:
            body.pop('error')
        if 'name' not in body:
            body['name'] = self.name

        if 'isSystem' in body:
            body['system'] = body.pop('isSystem')
        if 'type' in body:
            body['edge'] = body.pop('type') == 3
        if 'waitForSync' in body:
            body['sync'] = body.pop('waitForSync')
        if 'statusString' in body:
            body['status'] = body.pop('statusString')
        elif 'status' in body:
            body['status'] = self._get_status_string(body['status'])
        if 'globallyUniqueId' in body:
            body['global_id'] = body.pop('globallyUniqueId')
        if 'objectId' in body:
            body['id'] = body.pop('objectId')
        if 'cacheEnabled' in body:
            body['cache'] = body.pop('cacheEnabled')
        if 'doCompact' in body:
            body['compact'] = body.pop('doCompact')
        if 'isVolatile' in body:
            body['volatile'] = body.pop('isVolatile')
        if 'shardKeys' in body:
            body['shard_fields'] = body.pop('shardKeys')
        if 'replicationFactor' in body:
            body['replication_factor'] = body.pop('replicationFactor')
        if 'isSmart' in body:
            body['smart'] = body.pop('isSmart')
        if 'indexBuckets' in body:
            body['index_bucket_count'] = body.pop('indexBuckets')
        if 'journalSize' in body:
            body['journal_size'] = body.pop('journalSize')
        if 'numberOfShards' in body:
            body['shard_count'] = body.pop('numberOfShards')

        key_options = body.pop('keyOptions', {})
        if 'type' in key_options:
            body['key_generator'] = key_options['type']
        if 'increment' in key_options:
            body['key_increment'] = key_options['increment']
        if 'offset' in key_options:
            body['key_offset'] = key_options['offset']
        if 'allowUserKeys' in key_options:
            body['user_keys'] = key_options['allowUserKeys']
        if 'lastValue' in key_options:
            body['key_last_value'] = key_options['lastValue']

        return body

    def _extract_key(self, doc):
        """Return the document key from its ID or key.

        :param doc: Document ID or key.
        :type doc: str or unicode
        :return: Document key.
        :rtype: str or unicode
        :raise arango.exceptions.DocumentParseError: If input is malformed.
        """
        parts = doc.split('/', 1)
        if len(parts) == 2 and parts[0] != self.name:
            raise DocumentParseError(
                message='bad collection name "{}"'.format(parts[0]))
        return parts[-1]

    def _get_key_and_rev(self, doc):
        """Return the document key and rev from document ID, body or key.

        :param doc: Document ID, body or key.
        :type doc: str or unicode or dict
        :return: Document key and rev.
        :rtype: (str or unicode, str or unicode or None)
        :raise arango.exceptions.DocumentParseError: If input is malformed.
        """
        if is_str(doc):
            return self._extract_key(doc) if '/' in doc else doc, None
        elif '_key' in doc:
            return doc['_key'], doc.get('_rev')
        elif '_id' in doc:
            return self._extract_key(doc['_id']), doc.get('_rev')
        raise DocumentParseError(message='malformed document body')

    def _get_key(self, doc):
        """Return the document key from document ID, body or key.

        :param doc: Document ID, body or key.
        :type doc: str or unicode or dict
        :return: Document key.
        :rtype: str or unicode
        :raise arango.exceptions.DocumentParseError: If input is malformed.
        """
        if is_str(doc):
            return self._extract_key(doc) if '/' in doc else doc
        elif '_key' in doc:
            return doc['_key']
        elif '_id' in doc:
            return self._extract_key(doc['_id'])
        raise DocumentParseError(message='malformed document body')

    @property
    def name(self):
        """Return the name of the collection.

        :return: Collection name
        :rtype: str or unicode
        """
        return self._name

    @property
    def database(self):
        """Return the name of the database the collection belongs to.

        :return: Database name.
        :rtype: str or unicode
        """
        return self._conn.database

    def rename(self, new_name):
        """Rename the collection.

        :param new_name: New name for the collection.
        :type new_name: str or unicode
        :return: True if the rename was successful.
        :rtype: bool
        :raise arango.exceptions.CollectionRenameError: If rename fails.

        .. warning::
            Collection renames are not reflected immediately in async, batch
            or transaction API execution context.

        .. warning::
            Collection renames may not be reflected in the wrappers. It is
            recommended to use new collection wrapper objects after a rename.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rename'.format(self.name),
            data={'name': new_name},
            command='db.{}.rename({})'.format(self.name, new_name),
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionRenameError(resp)
            self._name = new_name
            return True

        return self._execute(request, response_handler)

    def properties(self):
        """Return the collection properties.

        :return: Collection properties.
        :rtype: dict
        :raise arango.exceptions.CollectionPropertiesError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/properties'.format(self.name),
            command='db.{}.properties()'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionPropertiesError(resp)
            return self._format_properties(resp.body)

        return self._execute(request, response_handler)

    def configure(self, sync=None, journal_size=None):
        """Set the collection properties.

        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param journal_size: Journal size.
        :type journal_size: int
        :return: New collection properties.
        :rtype: dict
        :raise arango.exceptions.CollectionConfigureError: If configure fails.
        """
        data = {}
        if sync is not None:
            data['waitForSync'] = sync
        if journal_size is not None:
            data['journalSize'] = journal_size

        request = Request(
            method='put',
            endpoint='/_api/collection/{}/properties'.format(self.name),
            data=data,
            command = 'db.{}.properties({})'.format(self.name, dumps(data)),
            read = self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionConfigureError(resp)
            return self._format_properties(resp.body)

        return self._execute(request, response_handler)

    def statistics(self):
        """Return the collection statistics.

        :return: Collection statistics.
        :rtype: dict
        :raise arango.exceptions.CollectionStatisticsError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/figures'.format(self.name),
            command='db.{}.figures()'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionStatisticsError(resp)

            stats = resp.body.get('figures', resp.body)
            if 'compactionStatus' in stats:
                stats['compaction_status'] = stats.pop('compactionStatus')
            if 'documentReferences' in stats:
                stats['document_refs'] = stats.pop('documentReferences')
            if 'lastTick' in stats:
                stats['last_tick'] = stats.pop('lastTick')
            if 'waitingFor' in stats:
                stats['waiting_for'] = stats.pop('waitingFor')
            if 'documentsSize' in stats:
                stats['documents_size'] = stats.pop('documentsSize')
            if 'uncollectedLogfileEntries' in stats:
                stats['uncollected_logfile_entries'] = \
                    stats.pop('uncollectedLogfileEntries')
            return stats

        return self._execute(request, response_handler)

    def revision(self):
        """Return the collection revision.

        :return: Collection revision.
        :rtype: str or unicode
        :raise arango.exceptions.CollectionRevisionError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/revision'.format(self.name),
            command='db.{}.revision()'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionRevisionError(resp)
            if is_dict(resp.body):
                return resp.body['revision']
            return resp.body

        return self._execute(request, response_handler)

    def checksum(self, with_rev=False, with_data=False):
        """Return the collection checksum.

        :param with_rev: Include document revisions in checksum calculations.
        :type with_rev: bool
        :param with_data: Include document data in checksum calculations.
        :type with_data: bool
        :return: Collection checksum.
        :rtype: str or unicode
        :raise arango.exceptions.CollectionChecksumError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/checksum'.format(self.name),
            params={'withRevision': with_rev, 'withData': with_data},
            command='db.{}.checksum()'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionChecksumError(resp)
            return resp.body['checksum']

        return self._execute(request, response_handler)

    def load(self):
        """Load the collection into memory.

        :return: True if the load operation was successful.
        :rtype: bool
        :raise arango.exceptions.CollectionLoadError: If load fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/load'.format(self.name),
            command='db.{}.load()'.format(self.name),
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionLoadError(resp)
            return True

        return self._execute(request, response_handler)

    def unload(self):
        """Unload the collection from memory.

        :return: True if the unload operation was successful.
        :rtype: bool
        :raise arango.exceptions.CollectionUnloadError: If unload fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/unload'.format(self.name),
            command='db.{}.load()'.format(self.name),
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionUnloadError(resp)
            return True

        return self._execute(request, response_handler)

    def rotate(self):
        """Rotate the collection journal.

        :return: True if the rotate operation was successful.
        :rtype: bool
        :raise arango.exceptions.CollectionRotateJournalError: If rotate fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rotate'.format(self.name),
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionRotateJournalError(resp)
            return True

        return self._execute(request, response_handler)

    def truncate(self):
        """Delete all documents in the collection.

        :return: True if the truncate was successful.
        :rtype: dict
        :raise arango.exceptions.CollectionTruncateError: If truncate fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/truncate'.format(self.name),
            command='db.{}.truncate()'.format(self.name),
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionTruncateError(resp)
            return True

        return self._execute(request, response_handler)

    def count(self):
        """Return the number of documents in the collection.

        :return: Number of documents.
        :rtype: int
        :raise arango.exceptions.DocumentCountError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/count'.format(self.name),
            command='db.{}.count()'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentCountError(resp)
            return resp.body if is_int(resp.body) else resp.body['count']

        return self._execute(request, response_handler)

    def has(self, document, rev=None, check_rev=True):
        """Check if a document exists in the collection.

        :param document: Document body, ID or key.
        :type document: dict or str or unicode
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **document** if present.
        :type rev: str or unicode
        :param check_rev: If set to True, the revision of **document** (if
            present) is compared against the revision of the target document.
        :type check_rev: bool
        :return: True if the document exists, False otherwise.
        :rtype: bool
        :raise arango.exceptions.DocumentInError: If check fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        key, _rev = self._get_key_and_rev(document)

        headers = {}
        if rev is None:
            rev = _rev
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        if self.context != 'transaction':
            command = None
        else:
            document = {'_key': key}
            if check_rev and rev is not None:
                document['_rev'] = rev
            command = 'db.{}.exists({})'.format(self.name, dumps(document))

        request = Request(
            method='get',
            endpoint='/_api/document/{}/{}'.format(self.name, key),
            headers=headers,
            command=command,
            read=self.name
        )

        def response_handler(resp):
            if resp.status_code in {304, 412}:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404 and resp.error_code == 1202:
                return False
            elif resp.is_success:
                return bool(resp.body)
            raise DocumentInError(resp)

        return self._execute(request, response_handler)

    def ids(self):
        """Return the IDs of all documents in the collection.

        :return: Cursor or list of document IDs.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentIDsError: If retrieval fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/all-keys',
            data={'collection': self.name, 'type': 'id'},
            command='db.{}.all().toArray().map(d => d._id)'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentIDsError(resp)
            return Cursor(self._conn, resp.body)

        return self._execute(request, response_handler)

    def keys(self):
        """Return the keys of all documents in the collection.

        :return: Cursor of document keys.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentKeysError: If retrieval fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/all-keys',
            data={'collection': self.name, 'type': 'key'},
            command='db.{}.all().toArray().map(d => d._key)'.format(self.name),
            read=self.name,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentKeysError(resp)
            return Cursor(self._conn, resp.body)

        return self._execute(request, response_handler)

    def all(self, skip=None, limit=None):
        """Return all documents in the collection using a server cursor.

        :param skip: Number of documents to skip.
        :type skip: int
        :param limit: Max number of documents fetched by the cursor.
        :type limit: int
        :return: Document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        """
        data = {'collection': self.name}
        if skip is not None:
            data['skip'] = skip
        if limit is not None:
            data['limit'] = limit

        command = 'db.{}.all().toArray().slice({}).slice(0, {})'.format(
            self.name,
            0 if skip is None else skip,
            -1 if limit is None else limit
        )
        request = Request(
            method='put',
            endpoint='/_api/simple/all',
            data=data,
            command=command,
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentGetError(resp)
            return Cursor(self._conn, resp.body)

        return self._execute(request, response_handler)

    def export(self,
               limit=None,
               count=False,
               batch_size=None,
               flush=None,
               flush_wait=None,
               ttl=None,
               filter_fields=None,
               filter_type='include'):
        """"Export all documents in the collection using a server cursor.

        :param flush: If set to True, flush the WAL prior to the export. If set
            to False, documents in WAL during export are not included by the
            server cursor.
        :type flush: bool
        :param flush_wait: Maximum wait time in seconds for the WAL flush.
        :type flush_wait: int
        :param count: Include the document count in the server cursor.
        :type count: bool
        :param batch_size: Maximum number of documents in the batch fetched by
            the cursor in one round trip.
        :type batch_size: int
        :param limit: Maximum number of documents fetched by the cursor.
        :type limit: int
        :param ttl: Time-to-live for the cursor on the server.
        :type ttl: int
        :param filter_fields: Fields used to filter documents.
        :type filter_fields: [str or unicode]
        :param filter_type: Allowed values are "include" or "exclude".
        :type filter_type: str or unicode
        :return: Document cursor.
        :rtype: arango.cursor.Cursor
        :raise arango.exceptions.DocumentGetError: If export fails.
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
            params={'collection': self.name},
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentGetError(resp)
            return Cursor(self._conn, resp.body, 'export')

        return self._execute(request, response_handler)

    def find(self, filters, offset=None, limit=None):
        """Return all documents that match the given filters.

        :param filters: Document filters.
        :type filters: dict
        :param offset: Number of documents to skip.
        :type offset: int
        :param limit: Maximum number of documents to return.
        :type limit: int
        :return: Document cursor.
        :rtype: arango.cursor.Cursor or list
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        """
        data = {'collection': self.name, 'example': filters}
        if offset is not None:
            data['skip'] = offset
        if limit is not None:
            data['limit'] = limit

        cmd = 'db.{}.byExample({}).toArray().slice({}).slice(0, {})'.format(
            self.name,
            dumps(filters),
            0 if offset is None else offset,
            -1 if limit is None else limit
        ) if self.context == 'transaction' else None

        request = Request(
            method='put',
            endpoint='/_api/simple/by-example',
            data=data,
            command=cmd,
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentGetError(resp)
            if is_list(resp.body):
                return resp.body
            return Cursor(self._conn, resp.body)

        return self._execute(request, response_handler)

    def get_many(self, documents):
        """Return multiple documents.

        :param documents: List of document bodies, keys, or IDs.
        :type documents: [str or unicode or dict]
        :return: Documents.
        :rtype: [dict]
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        """
        keys = [self._get_key(d) for d in documents]

        command = 'db.{}.document({})'.format(
            self.name,
            dumps(keys)
        ) if self.context == 'transaction' else None

        request = Request(
            method='put',
            endpoint='/_api/simple/lookup-by-keys',
            data={'collection': self.name, 'keys': keys},
            command=command,
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentGetError(resp)
            if is_dict(resp.body):
                return resp.body['documents']
            return resp.body

        return self._execute(request, response_handler)

    def random(self):
        """Return a random document from the collection.

        :return: A random document.
        :rtype: dict
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/any',
            data={'collection': self.name},
            command='db.{}.any()'.format(self.name),
            read=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentGetError(resp)
            return resp.body.get('document', resp.body)

        return self._execute(request, response_handler)

    ####################
    # Index Management #
    ####################

    def indexes(self):
        """Return the collection indexes.

        :return: Collection indexes.
        :rtype: [dict]
        :raise arango.exceptions.IndexListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/index',
            params={'collection': self.name}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise IndexListError(resp)

            indexes = []
            for index in resp.body['indexes']:
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

        return self._execute(request, response_handler)

    def _add_index(self, data):
        """Helper method for creating a new index."""
        request = Request(
            method='post',
            endpoint='/_api/index',
            data=data,
            params={'collection': self.name}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise IndexCreateError(resp)
            details = resp.body
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

        return self._execute(request, response_handler)

    def add_hash_index(self,
                       fields,
                       unique=None,
                       sparse=None,
                       deduplicate=None):
        """Create a new hash index.

        :param fields: Document fields to index.
        :type fields: [str or unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: If set to True, documents with None in the field
            are also indexed. If set to False, they are skipped.
        :type sparse: bool
        :param deduplicate: Whether inserting duplicate index values from the
            same document triggers unique constraint errors or not.
        :param deduplicate: bool
        :return: Details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If create fails.
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

        :param fields: Document fields to index.
        :type fields: [str or unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: If set to True, documents with None in the field
            are also indexed. If set to False, they are skipped.
        :type sparse: bool
        :param deduplicate: Whether inserting duplicate index values from the
            same document triggers unique constraint errors or not.
        :param deduplicate: bool
        :return: Details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If create fails.
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
        :type fields: str or unicode or [str or unicode]
        :param ordered: Whether the order is longitude then latitude.
        :type ordered: bool
        :return: Details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If create fails.
        """
        data = {'type': 'geo', 'fields': fields}
        if ordered is not None:
            data['geoJson'] = ordered
        return self._add_index(data)

    def add_fulltext_index(self, fields, min_length=None):
        """Create a fulltext index.

        :param fields: Document fields to index.
        :type fields: [str or unicode]
        :param min_length: Minimum number of characters to index.
        :type min_length: int
        :return: Details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If create fails.
        """
        data = {'type': 'fulltext', 'fields': fields}
        if min_length is not None:
            data['minLength'] = min_length
        return self._add_index(data)

    def add_persistent_index(self, fields, unique=None, sparse=None):
        """Create a persistent index.

        :param fields: Document fields to index.
        :type fields: [str or unicode]
        :param unique: Whether the index is unique.
        :type unique: bool
        :param sparse: Exclude documents that do not contain at least one of
            the indexed fields, or documents that have a value of None in
            any of the indexed fields.
        :type sparse: bool
        :return: Details on the new index.
        :rtype: dict
        :raise arango.exceptions.IndexCreateError: If create fails.

        .. warning::
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

        :param index_id: Index ID.
        :type index_id: str or unicode
        :param ignore_missing: Do not raise an exception on missing indexes.
        :type ignore_missing: bool
        :return: True if deleted successfully, False otherwise.
        :rtype: bool
        :raise arango.exceptions.IndexDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/index/{}/{}'.format(self.name, index_id)
        )

        def response_handler(resp):
            if resp.status_code == 404 and resp.error_code == 1212:
                if ignore_missing:
                    return False
                raise IndexDeleteError(resp)
            if not resp.is_success:
                raise IndexDeleteError(resp)
            return not resp.body['error']

        return self._execute(request, response_handler)


class Collection(Base):
    """ArangoDB collection.

    A collection consists of documents. It is uniquely identified by its name,
    which must consist only of alphanumeric, hyphen and underscore characters.

    Be default, collections use the traditional key generator, which generates
    key values in a non-deterministic fashion. A deterministic, auto-increment
    key generator is available as well.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    :param name: Collection name.
    :type name: str or unicode
    """

    def __init__(self, connection, executor, name):
        super(Collection, self).__init__(connection, executor, name)

    def __repr__(self):
        return '<Collection {}>'.format(self.name)

    def __getitem__(self, key):
        return self.get(key)

    def get(self, document, rev=None, check_rev=True):
        """Retrieve a document.

        :param document: Document body, ID or key.
        :type document: dict or str or unicode
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **document** if present.
        :type rev: str or unicode
        :param check_rev: If set to True, the revision of **document** (if
            present) is compared against the revision of the target document.
        :type check_rev: bool
        :return: Document or None if not found.
        :rtype: dict
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        :raise arango.exceptions.DocumentRevisionError: If rev does not match.
        """
        key, _rev = self._get_key_and_rev(document)

        headers = {}
        if rev is None:
            rev = _rev
        if rev is not None and check_rev:
            headers['If-Match'] = rev

        if self.context != 'transaction':
            command = None
        else:
            document = {'_key': key}
            if check_rev and rev is not None:
                document['_rev'] = rev
            command = 'db.{}.exists({})'.format(self.name, dumps(document))

        request = Request(
            method='get',
            endpoint='/_api/document/{}/{}'.format(self.name, key),
            headers=headers,
            command=command,
            read=self.name
        )

        def response_handler(resp):
            if resp.status_code in {304, 412}:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404 and resp.error_code == 1202:
                return None
            elif resp.is_success:
                return resp.body
            raise DocumentGetError(resp)

        return self._execute(request, response_handler)

    def insert(self, document, return_new=False, sync=None):
        """Insert a new document.

        :param document: Document to insert. If it contains the "_key" field,
            the value is used as the key of the new document (auto-generated
            otherwise). Any "_id" or "_rev" field is ignored.
        :type document: dict
        :param return_new: Include body of the new document in the result.
        :type return_new: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the insert (e.g. document key, revision).
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        params = {'returnNew': return_new}
        if sync is not None:
            params['waitForSync'] = sync

        command = 'db.{}.insert({},{})'.format(
            self.name,
            dumps(document),
            dumps(params)
        ) if self._context == 'transaction' else None

        request = Request(
            method='post',
            endpoint='/_api/document/{}'.format(self.name),
            data=document,
            params=params,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentInsertError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def insert_many(self, documents, return_new=False, sync=None):
        """Insert multiple documents into the collection.

        :param documents: List of new documents to insert. If they contain the
            "_key" fields, the values are used as the keys of the new documents
            (auto-generated otherwise). Any "_id" or "_rev" field is ignored.
        :type documents: [dict]
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the insert (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        params = {'returnNew': return_new}
        if sync is not None:
            params['waitForSync'] = sync

        command = 'db.{}.insert({},{})'.format(
            self.name,
            dumps(documents),
            dumps(params)
        ) if self._context == 'transaction' else None

        request = Request(
            method='post',
            endpoint='/_api/document/{}'.format(self.name),
            data=documents,
            params=params,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentInsertError(resp)

            results = []
            for result in resp.body:
                if '_id' in result:
                    results.append(result)
                else:
                    sub_resp = Response(
                        method=resp.method,
                        url=resp.url,
                        headers=resp.headers,
                        status_code=resp.status_code,
                        status_text=resp.status_text,
                        raw_body=result
                    )
                    results.append(DocumentInsertError(sub_resp))

            return results

        return self._execute(request, response_handler)

    def update(self,
               document,
               merge=True,
               keep_none=True,
               return_new=False,
               return_old=False,
               check_rev=True,
               sync=None):
        """Update a document.

        :param document: Partial or full document with the updated values. It
            must contain the "_key" field.
        :type document: dict
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new one overwriting the old one.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: Include body of the new document in the result.
        :type return_new: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" field in **document** (if
            present) is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the update (e.g. document key, revision).
        :rtype: dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        params = {
            'keepNull': keep_none,
            'mergeObjects': merge,
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self.context != 'transaction':
            command = None
        else:
            raw_string = dumps(document)
            command = 'db.{}.update({},{},{})'.format(
                self.name,
                raw_string,
                raw_string,
                dumps(params)
            )

        request = Request(
            method='patch',
            endpoint='/_api/document/{}/{}'.format(
                self.name, document['_key']
            ),
            data=document,
            params=params,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif not resp.is_success:
                raise DocumentUpdateError(resp)
            resp.body['_old_rev'] = resp.body.pop('_oldRev')
            return resp.body

        return self._execute(request, response_handler)

    def update_many(self,
                    documents,
                    merge=True,
                    keep_none=True,
                    return_new=False,
                    return_old=False,
                    check_rev=True,
                    sync=None):
        """Update multiple documents.

        :param documents: Partial or full documents with the updated values.
            They must contain the "_key" fields.
        :type documents: [dict]
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new ones overwriting the old ones.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" fields in **documents**
            (if present) are compared against the revisions of the target
            documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the update (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.

        .. warning::
            The size of returned result may be large depending on the input.
        """
        params = {
            'keepNull': keep_none,
            'mergeObjects': merge,
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self.context != 'transaction':
            command = None
        else:
            raw_string = dumps(documents)
            command = 'db.{}.update({},{},{})'.format(
                self.name,
                raw_string,
                raw_string,
                dumps(params)
            )

        request = Request(
            method='patch',
            endpoint='/_api/document/{}'.format(self.name),
            data=documents,
            params=params,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentUpdateError(resp)

            results = []
            for result in resp.body:
                if '_id' not in result:
                    # An error occurred with this particular document
                    sub_resp = Response(
                        method='patch',
                        url=resp.url,
                        headers=resp.headers,
                        status_code=resp.status_code,
                        status_text=resp.status_text,
                        raw_body=result,
                    )
                    # Single out revision error
                    if result['errorNum'] == 1200:
                        result = DocumentRevisionError(sub_resp)
                    else:
                        result = DocumentUpdateError(sub_resp)
                else:
                    result['_old_rev'] = result.pop('_oldRev')
                results.append(result)

            return results

        return self._execute(request, response_handler)

    def update_match(self,
                     filters,
                     body,
                     limit=None,
                     keep_none=True,
                     sync=None,
                     merge=True):
        """Update matching documents.

        :param filters: Document filters.
        :type filters: dict
        :param body: Document body with the updates.
        :type body: dict
        :param limit: Maximum number of documents to update. If the limit is
            lower than the number of matched documents, random documents are
            chosen. This parameter is not supported on sharded collections.
        :type limit: int
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Number of documents updated.
        :rtype: int
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new ones overwriting the old ones.
        :type merge: bool
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        """
        data = {
            'collection': self.name,
            'example': filters,
            'newValue': body,
            'keepNull': keep_none,
            'mergeObjects': merge
        }
        if limit is not None:
            data['limit'] = limit
        if sync is not None:
            data['waitForSync'] = sync

        command = 'db.{}.updateByExample({},{},{})'.format(
            self.name,
            dumps(filters),
            dumps(body),
            dumps(data)
        ) if self.context == 'transaction' else None

        request = Request(
            method='put',
            endpoint='/_api/simple/update-by-example',
            data=data,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentUpdateError(resp)
            if is_dict(resp.body):
                return resp.body['updated']
            return resp.body

        return self._execute(request, response_handler)

    def replace(self,
                document,
                return_new=False,
                return_old=False,
                check_rev=True,
                sync=None):
        """Replace a document.

        :param document: New document to replace the old one with. It must
            contain the "_key" field. Edge document must also contain "_from"
            and "_to" fields.
        :type document: dict
        :param return_new: Include body of the new document in the result.
        :type return_new: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" field in **document**
            is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the replace (e.g. document key, revision).
        :rtype: dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        params = {
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev,
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self.context != 'transaction':
            command = None
        else:
            raw_string = dumps(document)
            command = 'db.{}.replace({},{},{})'.format(
                self.name,
                raw_string,
                raw_string,
                dumps(params)
            )

        request = Request(
            method='put',
            endpoint='/_api/document/{}/{}'.format(
                self.name, document['_key']
            ),
            params=params,
            data=document,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            if not resp.is_success:
                raise DocumentReplaceError(resp)
            resp.body['_old_rev'] = resp.body.pop('_oldRev')
            return resp.body

        return self._execute(request, response_handler)

    def replace_many(self,
                     documents,
                     return_new=False,
                     return_old=False,
                     check_rev=True,
                     sync=None):
        """Replace multiple documents.

        :param documents: New documents to replace the old ones with. They must
            contain the "_key" fields. Edge documents must also contain "_from"
            and "_to" fields.
        :type documents: [dict]
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" fields in **documents**
            are compared against the revisions of the target documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the replace (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.

        .. warning::
            The size of returned result may be large depending on the input.
        """
        params = {
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self.context != 'transaction':
            command = None
        else:
            raw_string = dumps(documents)
            command = 'db.{}.replace({},{},{})'.format(
                self.name,
                raw_string,
                raw_string,
                dumps(params)
            )

        request = Request(
            method='put',
            endpoint='/_api/document/{}'.format(self.name),
            params=params,
            data=documents,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentReplaceError(resp)

            results = []
            for result in resp.body:
                if '_id' not in result:
                    # An error occurred with this particular document
                    sub_resp = Response(
                        method=resp.method,
                        url=resp.url,
                        headers=resp.headers,
                        status_code=resp.status_code,
                        status_text=resp.status_text,
                        raw_body=result
                    )
                    # Single out revision error
                    if result['errorNum'] == 1200:
                        result = DocumentRevisionError(sub_resp)
                    else:
                        result = DocumentReplaceError(sub_resp)
                else:
                    result['_old_rev'] = result.pop('_oldRev')
                results.append(result)

            return results

        return self._execute(request, response_handler)

    def replace_match(self, filters, body, limit=None, sync=None):
        """Replace matching documents.

        :param filters: Document filters.
        :type filters: dict
        :param body: New document body.
        :type body: dict
        :param limit: Maximum number of documents to replace. If the limit
            is lower than the number of matched documents, random documents
            are chosen.
        :type limit: int
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Number of documents replaced.
        :rtype: int
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        """
        data = {
            'collection': self.name,
            'example': filters,
            'newValue': body
        }
        if limit is not None:
            data['limit'] = limit
        if sync is not None:
            data['waitForSync'] = sync

        command = 'db.{}.replaceByExample({},{},{})'.format(
            self.name,
            dumps(filters),
            dumps(body),
            dumps(data)
        ) if self.context == 'transaction' else None

        request = Request(
            method='put',
            endpoint='/_api/simple/replace-by-example',
            data=data,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentReplaceError(resp)
            if is_dict(resp.body):
                return resp.body['replaced']
            return resp.body

        return self._execute(request, response_handler)

    def delete(self,
               document,
               ignore_missing=False,
               return_old=False,
               check_rev=True,
               sync=None):
        """Delete a document.

        :param document: Document body, ID or key.
        :type document: dict or str or unicode
        :param ignore_missing: Do not raise an exception on missing document.
        :type ignore_missing: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" field in **document** (if
            any) is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Results of the delete (e.g. document key, new revision),
            or False if the document was missing but ignored.
        :rtype: dict or bool
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        key, rev = self._get_key_and_rev(document)

        params = {
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev,
        }
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        command = 'db.{}.remove({},{})'.format(
            self.name,
            dumps({'_key': key, '_rev': rev}),
            dumps(params)
        ) if self.context == 'transaction' else None

        request = Request(
            method='delete',
            endpoint='/_api/document/{}/{}'.format(self.name, key),
            params=params,
            headers=headers,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(resp)
            elif not resp.is_success:
                raise DocumentDeleteError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def delete_many(self,
                    documents,
                    return_old=False,
                    check_rev=True,
                    sync=None):
        """Delete multiple documents.

        :param documents: Document bodies or keys to delete. They must contain
            the "_key" fields.
        :type documents: [dict] or [str or unicode]
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" fields in **documents**
            are compared against the revisions of the target documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the delete (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentDeleteError: If delete fails.

        .. warning::
            The size of returned result may be large depending on the input.
        """
        normalized_documents = []
        for doc in documents:
            key, rev = self._get_key_and_rev(doc)
            normalized_documents.append({'_key': key, '_rev': rev})

        params = {
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        command = 'db.{}.remove({},{})'.format(
            self.name,
            dumps(normalized_documents),
            dumps(params)
        ) if self.context == 'transaction' else None

        request = Request(
            method='delete',
            endpoint='/_api/document/{}'.format(self.name),
            params=params,
            data=normalized_documents,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentDeleteError(resp)

            results = []
            for result in resp.body:
                if '_id' not in result:
                    # An error occurred with this particular document
                    sub_resp = Response(
                        method=resp.method,
                        url=resp.url,
                        headers=resp.headers,
                        status_code=resp.status_code,
                        status_text=resp.status_text,
                        raw_body=result
                    )
                    # Single out revision errors
                    if result['errorNum'] == 1200:
                        result = DocumentRevisionError(sub_resp)
                    else:
                        result = DocumentDeleteError(sub_resp)
                results.append(result)

            return results

        return self._execute(request, response_handler)

    def delete_match(self, filters, limit=None, sync=None):
        """Delete matching documents.

        :param filters: Document filters.
        :type filters: dict
        :param limit: Maximum number of documents to delete. If the limit
            is lower than the number of matched documents, random documents
            are chosen.
        :type limit: int
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Number of documents deleted.
        :rtype: dict
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        """
        data = {'collection': self.name, 'example': filters}
        if sync is not None:
            data['waitForSync'] = sync
        if limit is not None:
            data['limit'] = limit

        command = 'db.{}.removeByExample({}, {})'.format(
            self.name,
            dumps(filters),
            dumps(data)
        ) if self.context == 'transaction' else None

        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-example',
            data=data,
            command=command,
            write=self.name
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentDeleteError(resp)
            if is_dict(resp.body):
                return resp.body['deleted']
            return resp.body

        return self._execute(request, response_handler)

    def import_bulk(self,
                    documents,
                    halt_on_error=True,
                    details=True,
                    from_prefix=None,
                    to_prefix=None,
                    overwrite=None,
                    on_duplicate=None,
                    sync=None):
        """Insert multiple documents into the collection.

        This is faster than :func:`arango.collection.Collection.insert_many`
        but does not return as much information.

        :param documents: List of the new documents to insert. Any "_id" or
            "_rev" fields are ignored.
        :type documents: [dict]
        :param halt_on_error: Halt the entire import on an error.
        :type halt_on_error: bool
        :param details: If set to True, the returned result will include an
            additional list of detailed error messages.
        :type details: bool
        :param from_prefix: String prefix prepended to the value of "_from"
            field in each edge document inserted. For example, prefix "foo"
            prepended to "_from": "bar" will result in "_from": "foo/bar".
            This parameter only applies to edge collections.
        :type from_prefix: str or unicode
        :param to_prefix: String prefix prepended to the value of "_to" field
            in edge document inserted. For example, prefix "foo" prepended to
            "_to": "bar" will result in "_to": "foo/bar". This parameter only
            applies to edge collections.
        :type to_prefix: str or unicode
        :param overwrite: If set to True, all existing documents in the
            collection are removed prior to the import. Indexes are still
            preserved.
        :type overwrite: bool
        :param on_duplicate: Action to take on unique key constraint violations
            (applies only to documents with "_key" fields). Allowed values are:

            .. code-block:: none

                "error"   : Do not import the new documents and count them as
                            errors (this is the default).

                "update"  : Update the existing documents while preserving any
                            fields missing in the new ones. This action may
                            fail on secondary unique key constraint violations.

                "replace" : Replace the existing documents with new ones. This
                            action may fail on secondary unique key constraint
                            violations

                "ignore"  : Do not import the new documents and count them as
                            ignored, as opposed to counting them as errors.

        :type on_duplicate: str or unicode
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: Result of the bulk import.
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If import fails.
        """
        params = {
            'type': 'array',
            'collection': self.name,
            'complete': halt_on_error,
            'details': details,
        }
        if halt_on_error is not None:
            params['complete'] = halt_on_error
        if details is not None:
            params['details'] = details
        if from_prefix is not None:
            params['fromPrefix'] = from_prefix
        if to_prefix is not None:
            params['toPrefix'] = to_prefix
        if overwrite is not None:
            params['overwrite'] = overwrite
        if on_duplicate is not None:
            params['onDuplicate'] = on_duplicate
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/import',
            data=documents,
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentInsertError(resp)
            return resp.body

        return self._execute(request, response_handler)


class EdgeCollection(Base):
    """ArangoDB edge collection.

    An edge collection consists of edge documents. It is uniquely identified
    by its name which must consist only of alphanumeric characters, hyphen and
    and underscore. Edge collections share their namespace with other types of
    collections.

    Documents in an edge collection are fully accessible from a standard
    collection. Managing documents through an edge collection, however, adds
    additional guarantees: all modifications are executed in transactions and
    edge documents are checked against the edge definitions on insert.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    :param graph: Graph name.
    :type graph: str or unicode
    :param name: Edge collection name.
    :type name: str or unicode
    """

    def __init__(self, connection, executor, graph, name):
        super(EdgeCollection, self).__init__(connection, executor, name)
        self._graph = graph

    def __repr__(self):
        return '<EdgeCollection {}>'.format(self.name)

    def __getitem__(self, key):
        return self.get(key)

    @property
    def graph(self):
        """Return the graph name.

        :return: Graph name.
        :rtype: str or unicode
        """
        return self._graph

    def get(self, edge, rev=None, check_rev=True):
        """Retrieve an edge document.

        :param edge: New document body, ID or key.
        :type edge: dict or str or unicode
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if present.
        :type rev: str or unicode
        :param check_rev: If set to True, the revision of **edge** (if present)
            is compared against the revision of the target edge document.
        :type check_rev: bool
        :return: Edge document or None if not found.
        :rtype: dict
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        key, _rev = self._get_key_and_rev(edge)

        headers = {}
        if rev is None:
            rev = _rev
        if rev is not None and check_rev:
            headers['If-Match'] = rev

        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self.name, key
            ),
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404 and resp.error_code == 1202:
                return None
            elif not resp.is_success:
                raise DocumentGetError(resp)
            return resp.body['edge']

        return self._execute(request, response_handler)

    def insert(self, edge, sync=None):
        """Insert a new edge document.

        :param edge: Edge document to insert. If it contains the "_key" field,
            the value is used as the key of the new edge document (otherwise it
            is auto-generated). Any "_id" or "_rev" field is ignored.
        :type edge: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: ID, revision and key of the new document.
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/edge/{}'.format(
                self._graph, self.name
            ),
            data=edge,
            params=params,

        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentInsertError(resp)
            return resp.body['edge']

        return self._execute(request, response_handler)

    def update(self, edge, keep_none=True, sync=None, check_rev=True):
        """Update an edge document.

        :param edge: Partial or full edge document with the updated values. It
            must contain the "_key" field.
        :type edge: dict
        :param keep_none: If set to True, fields with value None are retained
            in the document, otherwise they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param check_rev: If set to True, the "_rev" field in **edge** is
            compared against the revision of the target document.
        :type check_rev: bool
        :return: ID, revision and key of the updated document.
        :rtype: dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        params = {'keepNull': keep_none}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        rev = edge.get('_rev')
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self.name, edge['_key']
            ),
            data=edge,
            params=params,
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif not resp.is_success:
                raise DocumentUpdateError(resp)
            result = resp.body['edge']
            result['_old_rev'] = result.pop('_oldRev')
            return result

        return self._execute(request, response_handler)

    def replace(self, edge, sync=None, check_rev=True):
        """Replace an edge document.

        :param edge: New edge document to replace the old one with. It must
            contain the "_key" field.
        :type edge: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param check_rev: If set to True, the "_rev" field in **edge** is
            compared against the revision of the target document.
        :type check_rev: bool
        :return: ID, revision and key of the replaced document.
        :rtype: dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        rev = edge.get('_rev')
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self.name, edge['_key']
            ),
            data=edge,
            params=params,
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif not resp.is_success:
                raise DocumentReplaceError(resp)
            result = resp.body['edge']
            result['_old_rev'] = result.pop('_oldRev')
            return result

        return self._execute(request, response_handler)

    def delete(self, edge, ignore_missing=False, sync=None, check_rev=True):
        """Delete an edge document.

        :param edge: Edge document body, ID or key.
        :type edge: dict or str or unicode
        :param ignore_missing: Do not raise an exception on missing document.
        :type ignore_missing: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param check_rev: If set to True, the "_rev" field in **edge** (if any)
            is compared against the revision of the target document.
        :type check_rev: bool
        :return: True if document was deleted successfully, False otherwise.
        :rtype: bool
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        key, rev = self._get_key_and_rev(edge)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self.name, key
            ),
            params=params,
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404 and resp.error_code == 1202:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(resp)
            elif not resp.is_success:
                raise DocumentDeleteError(resp)
            return resp.body['removed']

        return self._execute(request, response_handler)

    # TODO ArangoDB 3.3.4 is throwing 501 ILLEGAL /_api/edges' not implemented
    # def edges(self, vertex, direction=None):
    #     """Return the edge documents coming in and out of the vertex.
    #
    #     :param vertex: Start vertex document body or ID.
    #     :type vertex: dict or str or unicode
    #     :param direction: The direction of the edges. Allowed values are "in"
    #         and "out". If not set, edges in both directions are returned.
    #     :type direction: str or unicode or None
    #     :return: List of edges and statistics.
    #     :rtype: dict
    #     :raise arango.exceptions.EdgeListError: If retrieval fails.
    #     """
    #     params = {}
    #     if direction is not None:
    #         params['direction'] = direction
    #     if isinstance(vertex, dict):
    #         params['vertex'] = vertex['_id']
    #     else:
    #         params['vertex'] = vertex
    #
    #     request = Request(
    #         method='delete',
    #         endpoint='/_api/edges/{}'.format(self.name),
    #         params=params
    #     )
    #
    #     def response_handler(resp):
    #         if not resp.is_success:
    #             raise EdgeListError(resp)
    #         return resp.body
    #
    #     return self._execute(request, response_handler)


class VertexCollection(Base):
    """ArangoDB vertex collection.

    A vertex collection consists of vertex documents. It is uniquely identified
    by its name, which must consist only of alphanumeric characters, hyphen and
    the underscore characters. Vertex collections share their namespace with
    other types of collections.

    The documents in a vertex collection are fully accessible from a standard
    collection. Managing documents through a vertex collection, however, adds
    additional guarantees: all modifications are executed in transactions and
    if a vertex is deleted all connected edges are also deleted.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    :param graph: Graph name.
    :type graph: str or unicode
    :param name: Vertex collection name.
    :type name: str or unicode
    """

    def __init__(self, connection, executor, graph, name):
        super(VertexCollection, self).__init__(connection, executor, name)
        self._graph = graph

    def __repr__(self):
        return '<VertexCollection {}>'.format(self.name)

    def __getitem__(self, key):
        return self.get(key)

    @property
    def graph(self):
        """Return the graph name.

        :return: Graph name.
        :rtype: str or unicode
        """
        return self._graph

    def get(self, vertex, rev=None, check_rev=True):
        """Retrieve a vertex document.

        :param vertex: Vertex document body, ID or key.
        :type vertex: dict or str or unicode
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if present.
        :type rev: str or unicode
        :param check_rev: If set to True, the revision of **vertex** (if
            present) is compared against the revision of the target vertex.
        :type check_rev: bool
        :return: Vertex document or None if not found.
        :rtype: dict
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        key, _rev = self._get_key_and_rev(vertex)

        headers = {}
        if rev is None:
            rev = _rev
        if rev is not None and check_rev:
            headers['If-Match'] = rev

        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self.name, key
            ),
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404 and resp.error_code == 1202:
                return None
            elif not resp.is_success:
                raise DocumentGetError(resp)
            return resp.body['vertex']

        return self._execute(request, response_handler)

    def insert(self, vertex, sync=None):
        """Insert a new vertex document.

        :param vertex: Vertex document to insert. If it contains the "_key"
            field, the value is used as the key of the new vertex document
            (auto-generated otherwise). Any "_id" or "_rev" field is ignored.
        :type vertex: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: ID, revision and key of the new document.
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex/{}'.format(
                self._graph, self.name
            ),
            data=vertex,
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DocumentInsertError(resp)
            return resp.body['vertex']

        return self._execute(request, response_handler)

    def update(self, vertex, keep_none=True, sync=None, check_rev=True):
        """Update a vertex document.

        :param vertex: Partial or full vertex document with updated values. It
            must contain the "_key" field.
        :type vertex: dict
        :param keep_none: If set to True, fields with value None are retained
            in the document, otherwise they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param check_rev: If set to True, the "_rev" field in **vertex** (if
            present) is compared against the revision of the target document.
        :type check_rev: bool
        :return: ID, revision and key of the updated document.
        :rtype: dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        params = {'keepNull': keep_none}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        rev = vertex.get('_rev')
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self.name, vertex['_key']
            ),
            data=vertex,
            params=params,
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif not resp.is_success:
                raise DocumentUpdateError(resp)
            result = resp.body['vertex']
            result['_old_rev'] = result.pop('_oldRev')
            return result

        return self._execute(request, response_handler)

    def replace(self, vertex, sync=None, check_rev=True):
        """Replace a vertex document.

        :param vertex: New vertex document to replace the old one with. It must
            contain the "_key" field.
        :type vertex: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param check_rev: If set to True, the "_rev" field in **vertex** (if
            present) is compared against the revision of the target document.
        :type check_rev: bool
        :return: ID, revision and key of the replaced document.
        :rtype: dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        rev = vertex.get('_rev')
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self.name, vertex['_key']
            ),
            params=params,
            data=vertex,
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif not resp.is_success:
                raise DocumentReplaceError(resp)
            result = resp.body['vertex']
            result['_old_rev'] = result.pop('_oldRev')
            return result

        return self._execute(request, response_handler)

    def delete(self, vertex, ignore_missing=False, sync=None, check_rev=True):
        """Delete a vertex document.

        :param vertex: Vertex document body, ID or key.
        :type vertex: dict or str or unicode
        :param ignore_missing: Do not raise an exception on missing document.
        :type ignore_missing: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param check_rev: If set to True, the "_rev" field in **vertex** is
            compared against the revision of the target document. (this only
            applicable when **vertex** is a document body and not a key).
        :type check_rev: bool
        :return: True if document was deleted successfully, False otherwise.
        :rtype: bool
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        :raise arango.exceptions.DocumentRevisionError: If revs do not match.
        """
        key, rev = self._get_key_and_rev(vertex)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        if check_rev and rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self.name, key
            ),
            params=params,
            headers=headers
        )

        def response_handler(resp):
            if resp.status_code == 412:
                raise DocumentRevisionError(resp)
            elif resp.status_code == 404 and resp.error_code == 1202:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(resp)
            if not resp.is_success:
                raise DocumentDeleteError(resp)
            return resp.body['removed']

        return self._execute(request, response_handler)
