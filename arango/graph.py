from __future__ import absolute_import, unicode_literals

from arango.api import APIWrapper
from arango.collections import EdgeCollection
from arango.collections import VertexCollection
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
from arango.utils import HTTP_OK


class Graph(APIWrapper):
    """ArangoDB graph.

    A graph consists of vertices and edges.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param name: The name of the graph.
    :type name: str | unicode
    """

    def __init__(self, requester, name):
        super(Graph, self).__init__(requester)
        self._name = name

    def __repr__(self):
        return '<ArangoDB graph "{}">'.format(self._name)

    @property
    def name(self):
        """Return the name of the graph.

        :return: The name of the graph.
        :rtype: str | unicode
        """
        return self._name

    def vertex_collection(self, name):
        """Return the vertex collection object.

        :param name: The name of the vertex collection.
        :type name: str | unicode
        :return: The vertex collection object.
        :rtype: arango.collections.vertex.VertexCollection
        """
        return VertexCollection(self._requester, self._name, name)

    def edge_collection(self, name):
        """Return the edge collection object.

        :param name: The name of the edge collection.
        :type name: str | unicode
        :return: The edge collection object.
        :rtype: arango.collections.edge.EdgeCollection
        """
        return EdgeCollection(self._requester, self._name, name)

    def properties(self):
        """Return the graph properties.

        :return: The graph properties.
        :rtype: dict
        :raise arango.exceptions.GraphPropertiesError: If the retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise GraphPropertiesError(res)
            record = res.body['graph']
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

        return self._execute_request(request, response_handler)

    ################################
    # Vertex Collection Management #
    ################################

    def orphan_collections(self):
        """Return the orphan vertex collections of the graph.

        :return: The names of the orphan vertex collections.
        :rtype: [str | unicode]
        :raise arango.exceptions.OrphanCollectionListError: If the retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise OrphanCollectionListError(res)
            return res.body['graph']['orphanCollections']

        return self._execute_request(request, response_handler)

    def vertex_collections(self):
        """Return the vertex collections of the graph.

        :return: The names of the vertex collections.
        :rtype: [str | unicode]
        :raise arango.exceptions.VertexCollectionListError: If the retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexCollectionListError(res)
            return res.body['collections']

        return self._execute_request(request, response_handler)

    def create_vertex_collection(self, name):
        """Create a vertex collection for the graph.

        :param name: The name of the new vertex collection to create.
        :type name: str | unicode
        :return: The vertex collection object.
        :rtype: arango.collections.vertex.VertexCollection
        :raise arango.exceptions.VertexCollectionCreateError: If the create
            fails.
        """
        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex'.format(self._name),
            data={'collection': name}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexCollectionCreateError(res)
            return VertexCollection(self._requester, self._name, name)

        return self._execute_request(request, response_handler)

    def delete_vertex_collection(self, name, purge=False):
        """Remove the vertex collection from the graph.

        :param name: The name of the vertex collection to remove.
        :type name: str | unicode
        :param purge: If set to True, the vertex collection is not just removed
            from the graph but deleted completely.
        :type purge: bool
        :return: True if the delete was successfully.
        :rtype: bool
        :raise arango.exceptions.VertexCollectionDeleteError: If the delete
            fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexCollectionDeleteError(res)
            return not res.body['error']

        return self._execute_request(request, response_handler)

    ##############################
    # Edge Definition Management #
    ##############################

    def edge_definitions(self):
        """Return the edge definitions of the graph.

        :return: The edge definitions of the graph.
        :rtype: [dict]
        :raise arango.exceptions.EdgeDefinitionListError: If the retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionListError(res)
            return [
                {
                    'name': edge_definition['collection'],
                    'to_collections': edge_definition['to'],
                    'from_collections': edge_definition['from']
                }
                for edge_definition in
                res.body['graph']['edgeDefinitions']
            ]

        return self._execute_request(request, response_handler)

    def create_edge_definition(self, name, from_collections, to_collections):
        """Create a new edge definition for the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: The name of the new edge collection.
        :type name: str | unicode
        :param from_collections: The names of the "from" vertex collections.
        :type from_collections: [str | unicode]
        :param to_collections: The names of the "to" vertex collections.
        :type to_collections: [str | unicode]
        :return: The edge collection object.
        :rtype: arango.collections.edge.EdgeCollection
        :raise arango.exceptions.EdgeDefinitionCreateError: If the create
            fails.
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

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionCreateError(res)
            return EdgeCollection(self._requester, self._name, name)

        return self._execute_request(request, response_handler)

    def replace_edge_definition(self, name, from_collections, to_collections):
        """Replace an edge definition in the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: The name of the edge definition to replace.
        :type name: str | unicode
        :param from_collections: The names of the "from" vertex collections.
        :type from_collections: [str | unicode]
        :param to_collections: The names of the "to" vertex collections.
        :type to_collections: [str | unicode]
        :return: True if the replace is successful.
        :rtype: bool
        :raise arango.exceptions.EdgeDefinitionReplaceError: If the replace
            fails.
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

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionReplaceError(res)
            return not res.body['error']

        return self._execute_request(request, response_handler)

    def delete_edge_definition(self, name, purge=False):
        """Delete an edge definition from the graph.

        An edge definition consists of an edge collection, "from" vertex
        collection(s), and "to" vertex collection(s).

        :param name: The name of the edge collection.
        :type name: str | unicode
        :param purge: If set to True, the edge collection is not just removed
            from the graph but deleted completely.
        :type purge: bool
        :return: True if the delete is successful.
        :rtype: bool
        :raise arango.exceptions.EdgeDefinitionDeleteError: If the delete
            fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/edge/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionDeleteError(res)
            return not res.body['error']

        return self._execute_request(request, response_handler)

    ####################
    # Graph Traversals #
    ####################

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

        :param start_vertex: The collection and the key of the start vertex in
            the format "{collection}/{key}".
        :type start_vertex: str | unicode
        :param direction: The direction of the traversal. Allowed values are
            "outbound" (default), "inbound" and "any".
        :type direction: str | unicode
        :param item_order: The item iteration order. Allowed values are
            "forward" (default) and "backward".
        :type item_order: str | unicode
        :param strategy: The traversal strategy. Allowed values are "dfs"
            (depth-first strategy) and "bfs" (breath-first strategy).
        :type strategy: str | unicode
        :param order: The traversal order. Allowed values are "preorder",
            "postorder", and "preorder-expander".
        :type order: str | unicode
        :param vertex_uniqueness: Specifies the uniqueness for visited
            vertices. Allowed values are "global", "path" or "none".
        :type vertex_uniqueness: str | unicode
        :param edge_uniqueness: Specifies the uniqueness for visited edges.
            Allowed values are "global", "path" or "none".
        :type edge_uniqueness: str | unicode
        :param min_depth: The minimum depth of the nodes to visit.
        :type min_depth: int
        :param max_depth: The maximum depth of the nodes to visit.
        :type max_depth: int
        :param max_iter: If set, halt the traversal after the maximum number of
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
        :return: The visited edges and vertices.
        :rtype: dict
        :raise arango.exceptions.GraphTraverseError: If the traversal fails.
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

        def response_handler(res):
            if res.status_code not in HTTP_OK:
                raise GraphTraverseError(res)
            return res.body['result']['visited']

        return self._execute_request(request, response_handler)
