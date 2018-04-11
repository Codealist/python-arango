from __future__ import absolute_import, unicode_literals

from arango.utils import split_id

__all__ = ['Graph']

from arango.api import APIWrapper
from arango.collection import EdgeCollection
from arango.collection import VertexCollection
from arango.exceptions import (
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionListError,
    EdgeDefinitionReplaceError,
    GraphPropertiesError,
    GraphTraverseError,
    OrphanCollectionListError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError,
)
from arango.request import Request


class Graph(APIWrapper):
    """ArangoDB graph.

    :param executor: API executor.
    :type executor: arango.executor.DefaultExecutor
    :param name: Graph name.
    :type name: str | unicode
    """

    def __init__(self, connection, executor, name):
        super(Graph, self).__init__(connection, executor)
        self._name = name

    def __repr__(self):
        return '<Graph {}>'.format(self._name)

    def _get_col_by_vertex(self, vertex):
        """Return the vertex collection for the given vertex document.

        :param vertex: Vertex document ID or body with "_id" field.
        :type vertex: str | unicode | dict
        :return: Vertex collection wrapper.
        :rtype: arango.collection.VertexCollection
        """
        return self.vertex_collection(split_id(vertex)[0])

    def _get_col_by_edge(self, edge):
        """Return the vertex collection for the given edge document.

        :param edge: Edge document ID or body with "_id" field.
        :type edge: str | unicode | dict
        :return: Edge collection wrapper.
        :rtype: arango.collection.EdgeCollection
        """
        return self.edge_collection(split_id(edge)[0])

    @property
    def name(self):
        """Return the graph name.

        :return: Graph name.
        :rtype: str | unicode
        """
        return self._name

    def properties(self):
        """Return the graph properties.

        :return: Graph properties.
        :rtype: dict
        :raise arango.exceptions.GraphPropertiesError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GraphPropertiesError(resp)
            record = resp.body['graph']
            return {
                'id': record['_id'],
                'name': record['name'],
                'revision': record['_rev'],
                'orphan_collections': record['orphanCollections'],
                'edge_definitions': [
                    {
                        'name': edge_definition['collection'],
                        'to_collections': edge_definition['to'],
                        'from_collections': edge_definition['from']
                    }
                    for edge_definition in record['edgeDefinitions']
                ],
                'smart': record.get('isSmart'),
                'smart_field': record.get('smartGraphAttribute'),
                'shard_count': record.get('numberOfShards')
            }

        return self._execute(request, response_handler)

    def orphan_collections(self):
        """Return the orphan vertex collections of the graph.

        :return: Names of orphan vertex collections.
        :rtype: [str | unicode]
        :raise arango.exceptions.OrphanCollectionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise OrphanCollectionListError(resp)
            return resp.body['graph']['orphanCollections']

        return self._execute(request, response_handler)

    ################################
    # Vertex Collection Management #
    ################################

    def vertex_collection(self, name):
        """Return the vertex collection wrapper.

        :param name: Name of vertex collection.
        :type name: str | unicode
        :return: Vertex collection wrapper.
        :rtype: arango.collection.VertexCollection
        """
        return VertexCollection(self._conn, self._executor, self._name, name)

    def vertex_collections(self):
        """Return the vertex collections of the graph.

        :return: Names of vertex collections.
        :rtype: [str | unicode]
        :raise arango.exceptions.VertexCollectionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex'.format(self._name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise VertexCollectionListError(resp)
            return resp.body['collections']

        return self._execute(request, response_handler)

    def create_vertex_collection(self, name):
        """Create a vertex collection for the graph.

        :param name: Name of new vertex collection.
        :type name: str | unicode
        :return: Vertex collection wrapper.
        :rtype: arango.collection.VertexCollection
        :raise arango.exceptions.VertexCollectionCreateError: If create fails.
        """
        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex'.format(self._name),
            data={'collection': name}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise VertexCollectionCreateError(resp)
            return self.vertex_collection(name)

        return self._execute(request, response_handler)

    def delete_vertex_collection(self, name, purge=False):
        """Remove the vertex collection from the graph.

        :param name: Name of vertex collection to remove.
        :type name: str | unicode
        :param purge: If set to True, the vertex collection is not just removed
            from the graph but also from the database completely.
        :type purge: bool
        :return: True if removal was successful.
        :rtype: bool
        :raise arango.exceptions.VertexCollectionDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise VertexCollectionDeleteError(resp)
            return True

        return self._execute(request, response_handler)

    ##############################
    # Edge Collection Management #
    ##############################

    def edge_collection(self, name):
        """Return the edge collection wrapper.

        :param name: Name of edge collection.
        :type name: str | unicode
        :return: Edge collection wrapper.
        :rtype: arango.collection.EdgeCollection
        """
        return EdgeCollection(self._conn, self._executor, self._name, name)

    def edge_definitions(self):
        """Return the edge definitions of the graph.

        :return: Edge definitions of the graph.
        :rtype: [dict]
        :raise arango.exceptions.EdgeDefinitionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionListError(resp)
            return [
                {
                    'name': edge_definition['collection'],
                    'to_collections': edge_definition['to'],
                    'from_collections': edge_definition['from']
                }
                for edge_definition in
                resp.body['graph']['edgeDefinitions']
            ]

        return self._execute(request, response_handler)

    def create_edge_definition(self, name, from_collections, to_collections):
        """Create a new edge definition (and edge collection) for the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s) and "to" vertex collection(s).

        :param name: Name of new edge definition. This is also used to create
            the associated edge collection in the same namespace as other
            collections.
        :type name: str | unicode
        :param from_collections: Names of "from" vertex collections.
        :type from_collections: [str | unicode]
        :param to_collections: Names of "to" vertex collections.
        :type to_collections: [str | unicode]
        :return: Edge collection wrapper.
        :rtype: arango.collection.EdgeCollection
        :raise arango.exceptions.EdgeDefinitionCreateError: If create fails.
        """
        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/edge'.format(self._name),
            data={
                'collection': name,
                'from': from_collections,
                'to': to_collections
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionCreateError(resp)
            return self.edge_collection(name)

        return self._execute(request, response_handler)

    def replace_edge_definition(self, name, from_collections, to_collections):
        """Replace an edge definition in the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: Name of the edge definition to replace.
        :type name: str | unicode
        :param from_collections: Names of "from" vertex collections.
        :type from_collections: [str | unicode]
        :param to_collections: Names of "to" vertex collections.
        :type to_collections: [str | unicode]
        :return: True if replace was successful.
        :rtype: bool
        :raise arango.exceptions.EdgeDefinitionReplaceError: If replace fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/edge/{}'.format(
                self._name, name
            ),
            data={
                'collection': name,
                'from': from_collections,
                'to': to_collections
            }
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionReplaceError(resp)
            return True

        return self._execute(request, response_handler)

    def delete_edge_definition(self, name, purge=False):
        """Delete an edge definition from the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: Name of edge definition.
        :type name: str | unicode
        :param purge: If set to True, the edge definition is not just removed
            from the graph but the edge collection is also deleted completely
            from the database.
        :type purge: bool
        :return: True if delete was successful.
        :rtype: bool
        :raise arango.exceptions.EdgeDefinitionDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/edge/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise EdgeDefinitionDeleteError(resp)
            return True

        return self._execute(request, response_handler)

    ###################
    # Graph Functions #
    ###################

    def traverse(self,
                 start_vertex,
                 direction='outbound',
                 item_order='forward',
                 strategy=None,
                 order=None,
                 edge_uniqueness=None,
                 vertex_uniqueness=None,
                 max_iter=None,
                 min_depth=None,
                 max_depth=None,
                 init_func=None,
                 sort_func=None,
                 filter_func=None,
                 visitor_func=None,
                 expander_func=None):
        """Traverse the graph and return the visited vertices and edges.

        :param start_vertex: Collection and key of the start vertex in the
            format "{collection}/{key}".
        :type start_vertex: str | unicode
        :param direction: Traversal direction. Allowed values are "outbound"
            (default), "inbound" and "any".
        :type direction: str | unicode
        :param item_order: Item iteration order. Allowed values are "forward"
            (default) and "backward".
        :type item_order: str | unicode
        :param strategy: Traversal strategy. Allowed values are "depthfirst"
            and "breadthfirst".
        :type strategy: str | unicode
        :param order: Traversal order. Allowed values are "preorder",
            "postorder", and "preorder-expander".
        :type order: str | unicode
        :param vertex_uniqueness: Specifies the uniqueness for visited
            vertices. Allowed values are "global", "path" or "none".
        :type vertex_uniqueness: str | unicode
        :param edge_uniqueness: Specifies the uniqueness for visited edges.
            Allowed values are "global", "path" or "none".
        :type edge_uniqueness: str | unicode
        :param min_depth: Minimum depth of the nodes to visit.
        :type min_depth: int
        :param max_depth: Max depth of the nodes to visit.
        :type max_depth: int
        :param max_iter: If set, halt the traversal after the Max number of
            iterations. This parameter can be used to prevent endless loops in
            cyclic graphs.
        :type max_iter: int
        :param init_func: Initialization function in Javascript with signature
            ``(config, result) -> void``. This function used to initialize any
            values in the result.
        :type init_func: str | unicode
        :param sort_func: Sorting function in Javascript with signature
            ``(left, right) -> integer``, which returns ``-1`` if ``left <
            right``, ``+1`` if ``left > right`` and ``0`` if ``left == right``.
        :type sort_func: str | unicode
        :param filter_func: Filter function in Javascript with signature
            ``(config, vertex, path) -> mixed``, where ``mixed`` can have one
            of the following values (or an array with multiple):

            .. code-block:: none

                "exclude"   : Do not visit the vertex.

                "prune"     : Do not follow the edges of the vertex.

                "undefined" : Visit the vertex and follow its edges.

        :type filter_func: str | unicode
        :param visitor_func: Visitor function in Javascript with signature
            ``(config, result, vertex, path, connected) -> void``. The return
            value is ignored, ``result`` is modified by reference, and
            ``connected`` is populated only when argument **order** is set to
            "preorder-expander".
        :type visitor_func: str | unicode
        :param expander_func: Expander function in Javascript with signature
            ``(config, vertex, path) -> mixed``. The function must return an
            array of connections for ``vertex``. Each connection is an object
            with attributes ``edge`` and ``vertex``.
        :type expander_func: str | unicode
        :return: Visited edges and vertices.
        :rtype: dict
        :raise arango.exceptions.GraphTraverseError: If traversal fails.
        """
        if strategy is not None:
            if strategy.lower() == 'dfs':
                strategy = 'depthfirst'
            elif strategy.lower() == 'bfs':
                strategy = 'breadthfirst'

        uniqueness = {}
        if vertex_uniqueness is not None:
            uniqueness['vertices'] = vertex_uniqueness
        if edge_uniqueness is not None:
            uniqueness['edges'] = edge_uniqueness

        data = {
            'startVertex': start_vertex,
            'graphName': self._name,
            'direction': direction,
            'strategy': strategy,
            'order': order,
            'itemOrder': item_order,
            'uniqueness': uniqueness or None,
            'maxIterations': max_iter,
            'minDepth': min_depth,
            'maxDepth': max_depth,
            'init': init_func,
            'filter': filter_func,
            'visitor': visitor_func,
            'sort': sort_func,
            'expander': expander_func
        }
        request = Request(
            method='post',
            endpoint='/_api/traversal',
            data={k: v for k, v in data.items() if v is not None}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GraphTraverseError(resp)
            return resp.body['result']['visited']

        return self._execute(request, response_handler)

    #####################
    # Vertex Management #
    #####################

    def vertex(self, vertex, rev=None, check_rev=True):
        """Return a vertex document.

        :param vertex: Vertex document ID, key or body. Document body must
            contain the "_id" or "_key" field.
        :type vertex: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if any.
        :type rev: str | unicode
        :param check_rev: If set to True, the revision of **vertex** (if any)
            is compared against the revision of the target vertex document.
        :type check_rev: bool
        :return: Vertex document or None if not found.
        :rtype: dict | None
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).get(
            vertex=vertex,
            rev=rev,
            check_rev=check_rev
        )

    def insert_vertex(self, collection, vertex, sync=None, silent=False):
        """Insert a new vertex document.

        :param collection: Name of the vertex collection.
        :type collection: str | unicode
        :param vertex: New vertex document to insert. If it has "_key" field,
            its value is used as the key of the new vertex document (otherwise
            it is auto-generated). Any "_id" or "_rev" field is ignored.
        :type vertex: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        return self.vertex_collection(collection).insert(
            vertex=vertex,
            sync=sync,
            silent=silent
        )

    def update_vertex(self,
                      vertex,
                      check_rev=True,
                      keep_none=True,
                      sync=None,
                      silent=False):
        """Update a vertex document.

        :param vertex: Partial or full vertex document with updated values. It
            must contain the "_key" or "_id" field.
        :type vertex: dict
        :param check_rev: If set to True, the "_rev" field in **vertex** (if
            any) is compared against the revision of the target document.
        :type check_rev: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. If set to False, they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).update(
            vertex=vertex,
            check_rev=check_rev,
            keep_none=keep_none,
            sync=sync,
            silent=silent
        )

    def replace_vertex(self, vertex, check_rev=True, sync=None, silent=False):
        """Replace a vertex document.

        :param vertex: New vertex document to replace the old one with. It must
            contain the "_key" or "_id" field.
        :type vertex: dict
        :param check_rev: If set to True, the "_rev" field in **vertex** (if
            any) is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).replace(
            vertex=vertex,
            check_rev=check_rev,
            sync=sync,
            silent=silent
        )

    def delete_vertex(self,
                      vertex,
                      rev=None,
                      check_rev=True,
                      ignore_missing=False,
                      sync=None):
        """Delete a vertex document.

        :param vertex: Vertex document ID, key or body. Document body must
            contain the "_id" or "_key" field.
        :type vertex: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **vertex** if any.
        :type rev: str | unicode
        :param check_rev: If set to True, the revision of **vertex** (if any)
            is compared against the revision of the target vertex.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised.
        :type ignore_missing: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: True if document was deleted successfully, False if document
            is missing and **ignore_missing** was set to True (does not apply
            in transactions).
        :rtype: bool
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_vertex(vertex).delete(
            vertex=vertex,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            sync=sync
        )

    ###################
    # Edge Management #
    ###################

    def edge(self, edge, rev=None, check_rev=True):
        """Return an edge document.

        :param edge: Edge document ID, key or body. Document body must contain
            the "_id" or "_key" field.
        :type edge: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if any.
        :type rev: str | unicode
        :param check_rev: If set to True, the revision of **edge** (if any) is
            compared against the revision of the target edge document.
        :type check_rev: bool
        :return: Edge document or None if not found.
        :rtype: dict | None
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).get(
            edge=edge,
            rev=rev,
            check_rev=check_rev
        )

    def insert_edge(self, collection, edge, sync=None, silent=False):
        """Insert a new edge document.

        :param collection: Name of the edge definition/collection.
        :type collection: str | unicode
        :param edge: New edge document to insert. It must contain "_from" and
            "_to" fields. If it has "_key" field, its value is used as the key
            of the new edge document (otherwise it is auto-generated). Any
            "_id" or "_rev" field is ignored.
        :type edge: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        return self.edge_collection(collection).insert(
            edge=edge,
            sync=sync,
            silent=silent
        )

    def update_edge(self,
                    edge,
                    check_rev=True,
                    keep_none=True,
                    sync=None,
                    silent=False):
        """Update an edge document.

        :param edge: Partial or full edge document with the updated values. It
            must contain the "_key" or "_id" field.
        :type edge: dict
        :param check_rev: If set to True, the "_rev" field in **edge** (if any)
            is compared against the revision of the target document.
        :type check_rev: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. If set to False, they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).update(
            edge=edge,
            check_rev=check_rev,
            keep_none=keep_none,
            sync=sync,
            silent=silent
        )

    def replace_edge(self, edge, check_rev=True, sync=None, silent=False):
        """Replace an edge document.

        :param edge: New edge document to replace the old one with. It must
            contain the "_key" or "_id" field. It must also contain the "_from"
            and "_to" fields.
        :type edge: dict
        :param check_rev: If set to True, the "_rev" field in **edge** (if any)
            is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).replace(
            edge=edge,
            check_rev=check_rev,
            sync=sync,
            silent=silent
        )

    def delete_edge(self,
                    edge,
                    rev=None,
                    check_rev=True,
                    ignore_missing=False,
                    sync=None):
        """Delete an edge document.

        :param edge: Edge document ID, key or body. Document body must contain
            the "_id" or "_key" field.
        :type edge: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **edge** if any.
        :type rev: str | unicode
        :param check_rev: If set to True, the revision of **edge** (if any)
            is compared against the revision of the target document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised.
        :type ignore_missing: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: True if document was deleted successfully, False if document
            is missing and **ignore_missing** was set to True (does not apply
            in transactions).
        :rtype: bool
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_edge(edge).delete(
            edge=edge,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            sync=sync
        )

    def link(self,
             collection,
             from_vertex,
             to_vertex,
             data=None,
             sync=None,
             silent=False):
        """Insert a new edge document linking the given vertices.

        :param collection: Name of the edge definition/collection.
        :type collection: str | unicode
        :param from_vertex: From vertex document body with "_id" field.
        :type from_vertex: dict
        :param to_vertex: To vertex document body with "_id" field.
        :type to_vertex: dict
        :param data: Any extra data for the new edge document (e.g. "_key").
        :type data: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        return self.edge_collection(collection).link(
            from_vertex=from_vertex,
            to_vertex=to_vertex,
            data=data,
            sync=sync,
            silent=silent
        )
