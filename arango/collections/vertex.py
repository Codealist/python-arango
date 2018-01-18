from __future__ import absolute_import, unicode_literals

from six import string_types

from arango import Request
from arango.collections import BaseCollection
from arango.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentUpdateError,
    DocumentReplaceError,
    DocumentRevisionError,
)
from arango.utils import HTTP_OK


class VertexCollection(BaseCollection):
    """ArangoDB vertex collection.

    A vertex collection consists of vertex documents. It is uniquely identified
    by its name, which must consist only of alphanumeric characters, hyphen and
    the underscore characters. Vertex collections share their namespace with
    other types of collections.

    The documents in a vertex collection are fully accessible from a standard
    collection. Managing documents through a vertex collection, however, adds
    additional guarantees: all modifications are executed in transactions and
    if a vertex is deleted all connected edges are also deleted.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param graph_name: The name of the graph.
    :type graph_name: str | unicode
    :param name: The name of the vertex collection.
    :type name: str | unicode
    """

    def __init__(self, requester, graph_name, name):
        super(VertexCollection, self).__init__(requester, name)
        self._graph_name = graph_name

    def __repr__(self):
        return (
            '<ArangoDB vertex collection "{}" in graph "{}">'
            .format(self._name, self._graph_name)
        )

    @property
    def graph_name(self):
        """Return the name of the graph.

        :return: The name of the graph
        :rtype: str | unicode
        """
        return self._graph_name

    def get(self, key, rev=None):
        """Retrieve a vertex document by its key.

        :param key: The document key.
        :type key: str | unicode
        :param rev: The document revision to be compared against the revision
            of the target document.
        :type rev: str | unicode
        :return: The vertex document or None if not found.
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If **rev** is given and
            its value does not match the target document revision.
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        headers = {}
        if rev is not None:
            headers['If-Match'] = rev

        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph_name, self._name, key
            ),
            headers=headers
        )

        def response_handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404 and res.error_code == 1202:
                return None
            elif res.status_code not in HTTP_OK:
                raise DocumentGetError(res)
            return res.body['vertex']

        return self._execute_request(request, response_handler)

    def insert(self, document, sync=None):
        """Insert a new vertex document.

        :param document: The document to insert.
        :type document: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The ID, revision and key of the new document.
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If the insert fails.

        .. note::
            If the "_key" field is present in **document**, its value is
            used as the key of the new document. If not present, the key is
            auto-generated.

        .. note::
            The "_id" and "_rev" fields are ignored if present in **document**.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex/{}'.format(
                self._graph_name, self._name
            ),
            data=document,
            params=params
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body['vertex']

        return self._execute_request(request, response_handler)

    def update(self, document, keep_none=True, sync=None):
        """Update a vertex document.

        :param document: The partial or full document with the updated values.
        :type document: dict
        :param keep_none: If set to True, fields with value None are retained
            in the document, otherwise they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The ID, revision and key of the updated document.
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If the "_rev" field is
            in **document** and its value does not match the revision of the
            target document.
        :raise arango.exceptions.DocumentUpdateError: If the update fails.

        .. note::
            The **document** must always contain the "_key" field.

        .. note::
            If the "_rev" field is present in **document**, its value is
            compared against the revision of the target document.
        """
        params = {'keepNull': keep_none}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph_name, self._name, document['_key']
            ),
            data=document,
            params=params,
            headers=headers
        )

        def response_handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            vertex = res.body['vertex']
            vertex['_old_rev'] = vertex.pop('_oldRev')
            return vertex

        return self._execute_request(request, response_handler)

    def replace(self, document, sync=None):
        """Replace a vertex document.

        :param document: The new document to replace the old one with.
        :type document: dict
        :param sync: wait for operation to sync to disk.
        :type sync: bool
        :return: The ID, revision and key of the replaced document.
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If the "_rev" field is
            in **document** and its value does not match the revision of the
            target document.
        :raise arango.exceptions.DocumentReplaceError: If the replace fails.

        .. note::
            The **document** must always contain the "_key" field.

        .. note::
            If the "_rev" field is present in **document**, its value is
            compared against the revision of the target document.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph_name, self._name, document['_key']
            ),
            params=params,
            data=document,
            headers=headers
        )

        def response_handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            vertex = res.body['vertex']
            vertex['_old_rev'] = vertex.pop('_oldRev')
            return vertex

        return self._execute_request(request, response_handler)

    def delete(self, document, ignore_missing=False, sync=None):
        """Delete an edge document.

        :param document: The document or its key.
        :type document: str | unicode | dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param ignore_missing: Do not raise an exception on missing document.
        :type ignore_missing: bool
        :return: True if document was deleted successfully, False otherwise.
        :rtype: bool
        :raise arango.exceptions.DocumentRevisionError: If the "_rev" field is
            in **document** and its value does not match the revision of the
            target document.
        :raise arango.exceptions.DocumentDeleteError: If the delete fails.

        .. note::
            The **document** must always contain the "_key" field.

        .. note::
            If the "_rev" field is present in **document**, its value is
            compared against the revision of the target document.
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        if isinstance(document, string_types):
            key = document
        else:
            revision = document.get('_rev')
            if revision is not None:
                headers['If-Match'] = revision
            key = document['_key']

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph_name, self._name, key
            ),
            params=params,
            headers=headers
        )

        def response_handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404 and res.error_code == 1202:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(res)
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body['removed']

        return self._execute_request(request, response_handler)
