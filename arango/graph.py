from __future__ import absolute_import, unicode_literals

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
    VertexCollectionListError
)
from arango.request import Request


class Graph(APIWrapper):
    """ArangoDB graph.

    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    :param name: Graph name.
    :type name: str or unicode
    """

    def __init__(self, connection, executor, name):
        super(Graph, self).__init__(connection, executor)
        self._name = name

    def __repr__(self):
        return '<Graph {}>'.format(self._name)

    @property
    def name(self):
        """Return the graph name.

        :return: Graph name.
        :rtype: str or unicode
        """
        return self._name

    @property
    def database(self):
        """Return the database name.

        :return: Database name.
        :rtype: str or unicode.
        """
        return self._conn.database

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
        :rtype: [str or unicode]
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
        :type name: str or unicode
        :return: Vertex collection wrapper.
        :rtype: arango.collection.VertexCollection
        """
        return VertexCollection(self._conn, self._executor, self._name, name)

    def vertex_collections(self):
        """Return the vertex collections of the graph.

        :return: Names of vertex collections.
        :rtype: [str or unicode]
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
        :type name: str or unicode
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
            return VertexCollection(
                self._conn, self._executor, self._name, name)

        return self._execute(request, response_handler)

    def delete_vertex_collection(self, name, purge=False):
        """Remove the vertex collection from the graph.

        :param name: Name of vertex collection to remove.
        :type name: str or unicode
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
            return not resp.body['error']

        return self._execute(request, response_handler)

    ##############################
    # Edge Collection Management #
    ##############################

    def edge_collection(self, name):
        """Return the edge collection wrapper.

        :param name: Name of edge collection.
        :type name: str or unicode
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
        :type name: str or unicode
        :param from_collections: Names of "from" vertex collections.
        :type from_collections: [str or unicode]
        :param to_collections: Names of "to" vertex collections.
        :type to_collections: [str or unicode]
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
            return EdgeCollection(self._conn, self._executor, self._name, name)

        return self._execute(request, response_handler)

    def replace_edge_definition(self, name, from_collections, to_collections):
        """Replace an edge definition in the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: Name of the edge definition to replace.
        :type name: str or unicode
        :param from_collections: Names of "from" vertex collections.
        :type from_collections: [str or unicode]
        :param to_collections: Names of "to" vertex collections.
        :type to_collections: [str or unicode]
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
            return not resp.body['error']

        return self._execute(request, response_handler)

    def delete_edge_definition(self, name, purge=False):
        """Delete an edge definition from the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: Name of edge definition.
        :type name: str or unicode
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
            return not resp.body['error']

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
        :type start_vertex: str or unicode
        :param direction: Traversal direction. Allowed values are "outbound"
            (default), "inbound" and "any".
        :type direction: str or unicode
        :param item_order: Item iteration order. Allowed values are "forward"
            (default) and "backward".
        :type item_order: str or unicode
        :param strategy: Traversal strategy. Allowed values are "depthfirst"
            and "breadthfirst".
        :type strategy: str or unicode
        :param order: Traversal order. Allowed values are "preorder",
            "postorder", and "preorder-expander".
        :type order: str or unicode
        :param vertex_uniqueness: Specifies the uniqueness for visited
            vertices. Allowed values are "global", "path" or "none".
        :type vertex_uniqueness: str or unicode
        :param edge_uniqueness: Specifies the uniqueness for visited edges.
            Allowed values are "global", "path" or "none".
        :type edge_uniqueness: str or unicode
        :param min_depth: Minimum depth of the nodes to visit.
        :type min_depth: int
        :param max_depth: Maximum depth of the nodes to visit.
        :type max_depth: int
        :param max_iter: If set, halt the traversal after the maximum number of
            iterations. This parameter can be used to prevent endless loops in
            cyclic graphs.
        :type max_iter: int
        :param init_func: Initialization function in Javascript with signature
            ``(config, result) -> void``. This function used to initialize any
            values in the result.
        :type init_func: str or unicode
        :param sort_func: Sorting function in Javascript with signature
            ``(left, right) -> integer``, which returns ``-1`` if ``left <
            right``, ``+1`` if ``left > right`` and ``0`` if ``left == right``.
        :type sort_func: str or unicode
        :param filter_func: Filter function in Javascript with signature
            ``(config, vertex, path) -> mixed``, where ``mixed`` can have one
            of the following values (or an array with multiple):

            .. code-block:: none

                "exclude"   : Do not visit the vertex.

                "prune"     : Do not follow the edges of the vertex.

                "undefined" : Visit the vertex and follow its edges.

        :type filter_func: str or unicode
        :param visitor_func: Visitor function in Javascript with signature
            ``(config, result, vertex, path, connected) -> void``. The return
            value is ignored, ``result`` is modified by reference, and
            ``connected`` is populated only when argument **order** is set to
            "preorder-expander".
        :type visitor_func: str or unicode
        :param expander_func: Expander function in Javascript with signature
            ``(config, vertex, path) -> mixed``. The function must return an
            array of connections for ``vertex``. Each connection is an object
            with attributes ``edge`` and ``vertex``.
        :type expander_func: str or unicode
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
