from __future__ import absolute_import, unicode_literals

from six import string_types

from arango.collection import EdgeCollection
from arango.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError,
    EdgeDefinitionCreateError,
    EdgeDefinitionDeleteError,
    EdgeDefinitionListError,
    EdgeDefinitionReplaceError,
    GraphListError,
    GraphCreateError,
    GraphDeleteError,
    GraphPropertiesError,
    GraphTraverseError,
    OrphanCollectionListError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError,
)
from tests.helpers import (
    assert_raises,
    clean_doc,
    extract,
    generate_col_name,
    generate_graph_name,
    generate_doc_key,
)


def test_graph_properties(graph, bad_graph, db):
    assert repr(graph) == '<Graph {}>'.format(graph.name)

    properties = graph.properties()
    assert properties['id'] == '_graphs/{}'.format(graph.name)
    assert properties['name'] == graph.name
    assert len(properties['edge_definitions']) == 1
    assert len(properties['orphan_collections']) == 2
    assert 'smart' in properties
    assert 'smart_field' in properties
    assert 'shard_count' in properties
    assert isinstance(properties['revision'], string_types)

    # Test properties with bad database
    with assert_raises(GraphPropertiesError):
        bad_graph.properties()

    new_graph_name = generate_graph_name()
    new_graph = db.create_graph(
        new_graph_name,
        # TODO only possible with enterprise edition
        # smart=True,
        # smart_field='foo',
        # shard_count=2
    )
    properties = new_graph.properties()
    assert properties['id'] == '_graphs/{}'.format(new_graph_name)
    assert properties['name'] == new_graph_name
    assert properties['edge_definitions'] == []
    assert properties['orphan_collections'] == []
    assert isinstance(properties['revision'], string_types)

    # TODO only possible with enterprise edition
    # assert properties['smart'] is True
    # assert properties['smart_field'] == 'foo'
    # assert properties['shard_count'] == 2


def test_graph_management(db, bad_db):
    # Test create graph
    graph_name = generate_graph_name()
    graph = db.create_graph(graph_name)
    assert graph.name == graph_name
    assert graph.db_name == db.name

    # Test create duplicate graph
    with assert_raises(GraphCreateError) as err:
        db.create_graph(graph_name)
    assert err.value.error_code == 1925

    # Test get graph
    result = db.graph(graph_name)
    assert result.name == graph.name
    assert result.db_name == graph.db_name

    # Test get graphs
    result = db.graphs()
    for entry in result:
        assert 'revision' in entry
        assert 'edge_definitions' in entry
        assert 'orphan_collections' in entry
    assert graph_name in extract('name', db.graphs())

    # Test get graphs with bad database
    with assert_raises(GraphListError):
        bad_db.graphs()

    # Test delete graph
    assert db.delete_graph(graph_name) is True
    assert graph_name not in extract('name', db.graphs())

    # Test delete missing graph
    with assert_raises(GraphDeleteError) as err:
        db.delete_graph(graph_name)
    assert err.value.error_code == 1924
    assert db.delete_graph(graph_name, ignore_missing=True) is False

    # Create a graph with vertex and edge collections and delete the graph
    graph = db.create_graph(graph_name)
    ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()

    graph.create_vertex_collection(fvcol_name)
    graph.create_vertex_collection(tvcol_name)
    graph.create_edge_definition(
        name=ecol_name,
        from_collections=[fvcol_name],
        to_collections=[tvcol_name]
    )
    collections = extract('name', db.collections())
    assert fvcol_name in collections
    assert tvcol_name in collections
    assert ecol_name in collections

    db.delete_graph(graph_name)
    collections = extract('name', db.collections())
    assert fvcol_name in collections
    assert tvcol_name in collections
    assert ecol_name in collections

    # Create a graph with vertex and edge collections and delete all
    graph = db.create_graph(graph_name)
    graph.create_edge_definition(
        name=ecol_name,
        from_collections=[fvcol_name],
        to_collections=[tvcol_name]
    )
    db.delete_graph(graph_name, drop_collections=True)
    collections = extract('name', db.collections())
    assert fvcol_name not in collections
    assert tvcol_name not in collections
    assert ecol_name not in collections


def test_vertex_collection_management(db, graph, bad_graph):
    # Test create valid from vertex collection
    fvcol_name = generate_col_name()
    fvcol = graph.create_vertex_collection(fvcol_name)
    assert fvcol.name == fvcol_name
    assert fvcol.graph == graph.name
    assert fvcol_name in repr(fvcol)
    assert fvcol_name in graph.vertex_collections()
    assert fvcol_name in graph.orphan_collections()
    assert fvcol_name in extract('name', db.collections())

    # Test create duplicate vertex collection
    with assert_raises(VertexCollectionCreateError) as err:
        graph.create_vertex_collection(fvcol_name)
    assert err.value.error_code == 1938
    assert fvcol_name in graph.vertex_collections()
    assert fvcol_name in graph.orphan_collections()
    assert fvcol_name in extract('name', db.collections())

    # Test create valid to vertex collection
    tvcol_name = generate_col_name()
    tvcol = graph.create_vertex_collection(tvcol_name)
    assert tvcol_name == tvcol_name
    assert tvcol.graph == graph.name
    assert tvcol_name in repr(tvcol)
    assert tvcol_name in graph.vertex_collections()
    assert tvcol_name in graph.orphan_collections()
    assert tvcol_name in extract('name', db.collections())

    # Test list vertex collection via bad database
    with assert_raises(VertexCollectionListError):
        bad_graph.vertex_collections()

    # Test list orphan collection via bad database
    with assert_raises(OrphanCollectionListError):
        bad_graph.orphan_collections()

    # Test delete missing vertex collection
    with assert_raises(VertexCollectionDeleteError) as err:
        graph.delete_vertex_collection(generate_col_name())
    assert err.value.error_code == 1926

    # Test delete to vertex collection with purge option
    assert graph.delete_vertex_collection(tvcol_name, purge=True) is True
    assert tvcol_name not in graph.vertex_collections()
    assert fvcol_name in extract('name', db.collections())
    assert tvcol_name not in extract('name', db.collections())

    # Test delete from vertex collection without purge option
    assert graph.delete_vertex_collection(fvcol_name, purge=False) is True
    assert fvcol_name not in graph.vertex_collections()
    assert fvcol_name in extract('name', db.collections())


def test_edge_definition_management(db, graph, bad_graph):
    ecol_name = generate_col_name()
    ecol = graph.create_edge_definition(ecol_name, [], [])
    assert isinstance(ecol, EdgeCollection)

    ecol = graph.edge_collection(ecol_name)
    assert ecol.name == ecol_name
    assert ecol.name in repr(ecol)
    assert ecol.graph == graph.name
    assert {
        'name': ecol_name,
        'from_collections': [],
        'to_collections': []
    } in graph.edge_definitions()
    assert ecol_name in extract('name', db.collections())

    # Test create duplicate edge definition
    with assert_raises(EdgeDefinitionCreateError) as err:
        graph.create_edge_definition(ecol_name, [], [])
    assert err.value.error_code == 1920

    # Test create edge definition with existing vertex collection
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    graph.create_vertex_collection(fvcol_name)
    graph.create_vertex_collection(tvcol_name)
    ecol_name = generate_col_name()
    ecol = graph.create_edge_definition(
        name=ecol_name,
        from_collections=[fvcol_name],
        to_collections=[tvcol_name]
    )
    assert ecol.name == ecol_name
    assert {
        'name': ecol_name,
        'from_collections': [fvcol_name],
        'to_collections': [tvcol_name]
    } in graph.edge_definitions()
    assert ecol_name in extract('name', db.collections())

    # Test create edge definition with missing vertex collection
    bad_vcol_name = generate_col_name()
    ecol_name = generate_col_name()
    ecol = graph.create_edge_definition(
        name=ecol_name,
        from_collections=[bad_vcol_name],
        to_collections=[bad_vcol_name]
    )
    assert ecol.name == ecol_name
    assert {
        'name': ecol_name,
        'from_collections': [bad_vcol_name],
        'to_collections': [bad_vcol_name]
    } in graph.edge_definitions()
    assert bad_vcol_name in graph.vertex_collections()
    assert bad_vcol_name not in graph.orphan_collections()
    assert bad_vcol_name in extract('name', db.collections())
    assert bad_vcol_name in extract('name', db.collections())

    # Test list edge definition with bad database
    with assert_raises(EdgeDefinitionListError):
        bad_graph.edge_definitions()

    # Test replace edge definition (happy path)
    assert graph.replace_edge_definition(
        name=ecol_name,
        from_collections=[tvcol_name],
        to_collections=[fvcol_name]
    ) is True
    assert {
        'name': ecol_name,
        'from_collections': [tvcol_name],
        'to_collections': [fvcol_name]
    } in graph.edge_definitions()

    # Test replace missing edge definition
    bad_ecol_name = generate_col_name()
    with assert_raises(EdgeDefinitionReplaceError):
        graph.replace_edge_definition(
            name=bad_ecol_name,
            from_collections=[],
            to_collections=[fvcol_name]
        )

    # Test delete missing edge definition
    with assert_raises(EdgeDefinitionDeleteError) as err:
        graph.delete_edge_definition(bad_ecol_name)
    assert err.value.error_code == 1930

    # Test delete existing edge definition with purge
    assert graph.delete_edge_definition(ecol_name, purge=True) is True
    assert {
        'name': ecol_name,
        'from_collections': [tvcol_name],
        'to_collections': [fvcol_name]
    } not in graph.edge_definitions()
    assert ecol_name not in extract('name', db.collections())


def test_create_graph_with_edge_definition(db):
    new_graph_name = generate_graph_name()
    new_ecol_name = generate_col_name()
    fvcol_name = generate_col_name()
    tvcol_name = generate_col_name()
    ovcol_name = generate_col_name()

    edge_definition = {
        'name': new_ecol_name,
        'from_collections': [fvcol_name],
        'to_collections': [tvcol_name]
    }
    new_graph = db.create_graph(
        new_graph_name,
        edge_definitions=[edge_definition],
        orphan_collections=[ovcol_name]
    )
    assert ovcol_name in new_graph.orphan_collections()
    assert edge_definition in new_graph.edge_definitions()


def test_vertex_management(fvcol, bad_fvcol, fvdocs):
    # Test insert vertex with no key
    result = fvcol.insert({})
    assert result['_key'] in fvcol
    assert len(fvcol) == 1
    fvcol.truncate()

    vertex = fvdocs[0]
    key = vertex['_key']

    # Test insert first valid vertex
    result = fvcol.insert(vertex)
    assert result['_key'] == key
    assert '_rev' in result
    assert vertex in fvcol and key in fvcol
    assert len(fvcol) == 1
    assert fvcol[key]['val'] == vertex['val']

    # Test insert duplicate vertex
    with assert_raises(DocumentInsertError) as err:
        fvcol.insert(vertex)
    assert err.value.error_code == 1210
    assert len(fvcol) == 1

    vertex = fvdocs[1]
    key = vertex['_key']

    # Test insert second valid vertex
    result = fvcol.insert(vertex, sync=True)
    assert result['_key'] == key
    assert vertex in fvcol and key in fvcol
    assert len(fvcol) == 2
    assert fvcol[key]['val'] == vertex['val']

    vertex = fvdocs[2]
    key = vertex['_key']

    # Test insert third valid vertex with silent set to True
    assert fvcol.insert(vertex, silent=True) is True
    assert len(fvcol) == 3
    assert fvcol[key]['val'] == vertex['val']

    # Test get missing vertex
    assert fvcol.get(generate_doc_key()) is None

    # Test get existing edge by body with "_key" field
    result = fvcol.get({'_key': key})
    assert clean_doc(result) == vertex

    # Test get existing edge by body with "_id" field
    result = fvcol.get({'_id': fvcol.name + '/' + key})
    assert clean_doc(result) == vertex

    # Test get existing vertex by key
    result = fvcol.get(key)
    assert clean_doc(result) == vertex

    # Test get existing vertex by ID
    result = fvcol.get(fvcol.name + '/' + key)
    assert clean_doc(result) == vertex

    # Test get existing vertex with bad revision
    old_rev = result['_rev']
    with assert_raises(DocumentRevisionError) as err:
        fvcol.get(key, rev=old_rev + '1', check_rev=True)
    assert err.value.error_code == 1903

    # Test get existing vertex with bad database
    with assert_raises(DocumentGetError) as err:
        bad_fvcol.get(key)
    assert err.value.error_code == 1228

    # Test update vertex with a single field change
    assert 'foo' not in fvcol.get(key)
    result = fvcol.update({'_key': key, 'foo': 100})
    assert result['_key'] == key
    assert fvcol[key]['foo'] == 100
    old_rev = fvcol[key]['_rev']

    # Test update vertex with silent set to True
    assert 'bar' not in fvcol[vertex]
    assert fvcol.update({'_key': key, 'bar': 200}, silent=True) is True
    assert fvcol[vertex]['bar'] == 200
    assert fvcol[vertex]['_rev'] != old_rev
    old_rev = fvcol[key]['_rev']

    # Test update vertex with multiple field changes
    result = fvcol.update({'_key': key, 'foo': 200, 'bar': 300})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 300
    old_rev = result['_rev']

    # Test update vertex with correct revision
    result = fvcol.update({'_key': key, '_rev': old_rev, 'bar': 400})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400
    old_rev = result['_rev']

    # Test update vertex with bad revision
    new_rev = old_rev + '1'
    with assert_raises(DocumentRevisionError) as err:
        fvcol.update({'_key': key, '_rev': new_rev, 'bar': 500})
    assert err.value.error_code == 1903
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400

    # Test update vertex in missing vertex collection
    with assert_raises(DocumentUpdateError) as err:
        bad_fvcol.update({'_key': key, 'bar': 500})
    assert err.value.error_code == 1228
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400

    # Test update vertex with sync set to True
    result = fvcol.update({'_key': key, 'bar': 500}, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 500
    old_rev = result['_rev']

    # Test update vertex with keep_none set to True
    result = fvcol.update({'_key': key, 'bar': None}, keep_none=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] is None
    old_rev = result['_rev']

    # Test update vertex with keep_none set to False
    result = fvcol.update({'_key': key, 'foo': None}, keep_none=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert 'foo' not in fvcol[key]
    assert fvcol[key]['bar'] is None

    # Test replace vertex with a single field change
    result = fvcol.replace({'_key': key, 'baz': 100})
    assert result['_key'] == key
    assert 'foo' not in fvcol[key]
    assert 'bar' not in fvcol[key]
    assert fvcol[key]['baz'] == 100
    old_rev = result['_rev']

    # Test replace vertex with silent set to True
    assert fvcol.replace({'_key': key, 'bar': 200}, silent=True) is True
    assert 'foo' not in fvcol[key]
    assert 'baz' not in fvcol[vertex]
    assert fvcol[vertex]['bar'] == 200
    assert len(fvcol) == 3
    assert fvcol[vertex]['_rev'] != old_rev
    old_rev = fvcol[vertex]['_rev']

    # Test replace vertex with multiple field changes
    vertex = {'_key': key, 'foo': 200, 'bar': 300}
    result = fvcol.replace(vertex)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert clean_doc(fvcol[key]) == vertex
    old_rev = result['_rev']

    # Test replace vertex with correct revision
    vertex = {'_key': key, '_rev': old_rev, 'bar': 500}
    result = fvcol.replace(vertex)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert clean_doc(fvcol[key]) == clean_doc(vertex)
    old_rev = result['_rev']

    # Test replace vertex with bad revision
    new_rev = old_rev + '10'
    vertex = {'_key': key, '_rev': new_rev, 'bar': 600}
    with assert_raises(DocumentRevisionError) as err:
        fvcol.replace(vertex)
    assert err.value.error_code == 1903
    assert fvcol[key]['bar'] == 500
    assert 'foo' not in fvcol[key]

    # Test replace vertex with bad database
    with assert_raises(DocumentReplaceError) as err:
        bad_fvcol.replace({'_key': key, 'bar': 600})
    assert err.value.error_code == 1228
    assert fvcol[key]['bar'] == 500
    assert 'foo' not in fvcol[key]

    # Test replace vertex with sync set to True
    vertex = {'_key': key, 'bar': 400, 'foo': 200}
    result = fvcol.replace(vertex, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400

    # Test delete vertex with bad revision
    old_rev = fvcol[key]['_rev']
    vertex['_rev'] = old_rev + '1'
    with assert_raises(DocumentRevisionError) as err:
        fvcol.delete(vertex, check_rev=True)
    assert err.value.error_code == 1903
    vertex['_rev'] = old_rev
    assert vertex in fvcol

    # Test delete missing vertex
    bad_key = generate_doc_key()
    with assert_raises(DocumentDeleteError) as err:
        fvcol.delete(bad_key, ignore_missing=False)
    assert err.value.error_code == 1202
    if fvcol.context != 'transaction':
        assert fvcol.delete(bad_key, ignore_missing=True) is False

    # Test delete existing vertex with sync set to True
    assert fvcol.delete(vertex, sync=True, check_rev=False) is True
    assert fvcol[vertex] is None
    assert vertex not in fvcol
    assert len(fvcol) == 2
    fvcol.truncate()


def test_vertex_management_via_graph(graph, fvcol):
    # Test insert vertex via graph object
    result = graph.insert_vertex(fvcol.name, {})
    assert result['_key'] in fvcol
    assert len(fvcol) == 1
    vertex_id = result['_id']

    # Test get vertex via graph object
    assert graph.vertex(vertex_id)['_id'] == vertex_id

    # Test update vertex via graph object
    result = graph.update_vertex({'_id': vertex_id, 'foo': 100})
    assert result['_id'] == vertex_id
    assert fvcol[vertex_id]['foo'] == 100

    # Test replace vertex via graph object
    result = graph.replace_vertex({'_id': vertex_id, 'bar': 200})
    assert result['_id'] == vertex_id
    assert 'foo' not in fvcol[vertex_id]
    assert fvcol[vertex_id]['bar'] == 200

    # Test delete vertex via graph object
    assert graph.delete_vertex(vertex_id) is True
    assert vertex_id not in fvcol
    assert len(fvcol) == 0


def test_edge_management(ecol, bad_ecol, edocs, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        fvcol.insert(vertex)
    for vertex in tvdocs:
        tvcol.insert(vertex)

    edge = edocs[0]
    key = edge['_key']

    # Test insert edge with no key
    result = ecol.insert({'_from': edge['_from'], '_to': edge['_to']})
    assert result['_key'] in ecol
    assert len(ecol) == 1
    ecol.truncate()

    # Test insert first valid edge
    result = ecol.insert(edge)
    assert result['_key'] == key
    assert '_rev' in result
    assert edge in ecol and key in ecol
    assert len(ecol) == 1
    assert ecol[key]['_from'] == edge['_from']
    assert ecol[key]['_to'] == edge['_to']

    # Test insert duplicate edge
    with assert_raises(DocumentInsertError) as err:
        assert ecol.insert(edge)
    assert err.value.error_code == 1906
    assert len(ecol) == 1

    edge = edocs[1]
    key = edge['_key']

    # Test insert second valid edge with silent set to True
    assert ecol.insert(edge, sync=True, silent=True) is True
    assert edge in ecol and key in ecol
    assert len(ecol) == 2
    assert ecol[key]['_from'] == edge['_from']
    assert ecol[key]['_to'] == edge['_to']

    # Test insert third valid edge using link method
    from_vertex = fvcol.get(fvdocs[2])
    to_vertex = tvcol.get(tvdocs[2])
    result = ecol.link(from_vertex, to_vertex, sync=False)
    assert result['_key'] in ecol
    assert len(ecol) == 3

    # Test insert fourth valid edge using link method
    from_vertex = fvcol.get(fvdocs[2])
    to_vertex = tvcol.get(tvdocs[0])
    assert ecol.link(
        from_vertex,
        to_vertex,
        {'_key': 'foo'},
        sync=True,
        silent=True
    ) is True
    assert 'foo' in ecol
    assert len(ecol) == 4

    # Test get missing vertex
    bad_document_key = generate_doc_key()
    assert ecol.get(bad_document_key) is None

    # Test get existing edge by body with "_key" field
    result = ecol.get({'_key': key})
    assert clean_doc(result) == edge

    # Test get existing edge by body with "_id" field
    result = ecol.get({'_id': ecol.name + '/' + key})
    assert clean_doc(result) == edge

    # Test get existing edge by key
    result = ecol.get(key)
    assert clean_doc(result) == edge

    # Test get existing edge by ID
    result = ecol.get(ecol.name + '/' + key)
    assert clean_doc(result) == edge

    # Test get existing edge with bad revision
    old_rev = result['_rev']
    with assert_raises(DocumentRevisionError) as err:
        ecol.get(key, rev=old_rev + '1')
    assert err.value.error_code == 1903

    # Test get existing vertex with bad database
    with assert_raises(DocumentGetError) as err:
        bad_ecol.get(key)
    assert err.value.error_code == 1228

    # Test update edge with a single field change
    assert 'foo' not in ecol.get(key)
    result = ecol.update({'_key': key, 'foo': 100})
    assert result['_key'] == key
    assert ecol[key]['foo'] == 100
    old_rev = ecol[key]['_rev']

    # Test update edge with multiple field changes
    result = ecol.update({'_key': key, 'foo': 200, 'bar': 300})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 300
    old_rev = result['_rev']

    # Test update edge with correct revision
    result = ecol.update({'_key': key, '_rev': old_rev, 'bar': 400})
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 400
    old_rev = result['_rev']

    # Test update edge with bad revision
    new_rev = old_rev + '1'
    with assert_raises(DocumentRevisionError):
        ecol.update({'_key': key, '_rev': new_rev, 'bar': 500})
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 400

    # Test update edge in missing edge collection
    with assert_raises(DocumentUpdateError) as err:
        bad_ecol.update({'_key': key, 'bar': 500})
    assert err.value.error_code == 1228
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 400

    # Test update edge with sync option
    result = ecol.update({'_key': key, 'bar': 500}, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 500
    old_rev = result['_rev']

    # Test update edge with silent option
    assert ecol.update({'_key': key, 'bar': 600}, silent=True) is True
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 600
    assert ecol[key]['_rev'] != old_rev
    old_rev = ecol[key]['_rev']

    # Test update edge without keep_none option
    result = ecol.update({'_key': key, 'bar': None}, keep_none=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] is None
    old_rev = result['_rev']

    # Test update edge with keep_none option
    result = ecol.update({'_key': key, 'foo': None}, keep_none=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert 'foo' not in ecol[key]
    assert ecol[key]['bar'] is None

    # Test replace edge with a single field change
    edge['foo'] = 100
    result = ecol.replace(edge)
    assert result['_key'] == key
    assert ecol[key]['foo'] == 100
    old_rev = ecol[key]['_rev']

    # Test replace edge with silent set to True
    edge['bar'] = 200
    assert ecol.replace(edge, silent=True) is True
    assert ecol[key]['foo'] == 100
    assert ecol[key]['bar'] == 200
    assert ecol[key]['_rev'] != old_rev
    old_rev = ecol[key]['_rev']

    # Test replace edge with multiple field changes
    edge['foo'] = 200
    edge['bar'] = 300
    result = ecol.replace(edge)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 300
    old_rev = result['_rev']

    # Test replace edge with correct revision
    edge['foo'] = 300
    edge['bar'] = 400
    edge['_rev'] = old_rev
    result = ecol.replace(edge)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 400
    old_rev = result['_rev']

    # Test replace edge with bad revision
    edge['bar'] = 500
    edge['_rev'] = old_rev + key
    with assert_raises(DocumentRevisionError) as err:
        ecol.replace(edge)
    assert err.value.error_code == 1903
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 400

    # Test replace edge with bad database
    with assert_raises(DocumentReplaceError) as err:
        bad_ecol.replace(edge)
    assert err.value.error_code == 1228
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 400

    # Test replace edge with sync option
    result = ecol.replace(edge, sync=True, check_rev=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 500

    # Test delete edge with bad revision
    old_rev = ecol[key]['_rev']
    edge['_rev'] = old_rev + '1'
    with assert_raises(DocumentRevisionError) as err:
        ecol.delete(edge, check_rev=True)
    assert err.value.error_code == 1903
    edge['_rev'] = old_rev
    assert edge in ecol

    # Test delete missing edge
    with assert_raises(DocumentDeleteError) as err:
        ecol.delete(bad_document_key, ignore_missing=False)
    assert err.value.error_code == 1202
    assert not ecol.delete(bad_document_key, ignore_missing=True)

    # Test delete existing edge with sync set to True
    assert ecol.delete(edge, sync=True, check_rev=False) is True
    assert ecol[edge] is None
    assert edge not in ecol
    ecol.truncate()


def test_edge_management_via_graph(graph, ecol, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        fvcol.insert(vertex)
    for vertex in tvdocs:
        tvcol.insert(vertex)
    ecol.truncate()

    from_vertex = fvcol.random()
    to_vertex = tvcol.random()

    # Test insert edge via graph object
    result = graph.insert_edge(
        ecol.name,
        {'_from': from_vertex['_id'], '_to': to_vertex['_id']}
    )
    assert result['_key'] in ecol
    assert len(ecol) == 1

    # Test link vertices via graph object
    result = graph.link(ecol.name, from_vertex, to_vertex)
    assert result['_key'] in ecol
    assert len(ecol) == 2
    edge_id = result['_id']

    # Test get edge via graph object
    assert graph.edge(edge_id)['_id'] == edge_id

    # Test update edge via graph object
    result = graph.update_edge({'_id': edge_id, 'foo': 100})
    assert result['_id'] == edge_id
    assert ecol[edge_id]['foo'] == 100

    # Test replace edge via graph object
    result = graph.replace_edge({
        '_id': edge_id,
        '_from': from_vertex['_id'],
        '_to': to_vertex['_id'],
        'bar': 200
    })
    assert result['_id'] == edge_id
    assert 'foo' not in ecol[edge_id]
    assert ecol[edge_id]['bar'] == 200

    # Test delete edge via graph object
    assert graph.delete_edge(edge_id) is True
    assert edge_id not in ecol
    assert len(ecol) == 1


# TODO ArangoDB 3.3.x is throwing 501 ILLEGAL /_api/edges' not implemented
# def test_vertex_edges(db):
#     # Create test graph, vertex and edge collections
#     school = db.create_graph(generate_graph_name())
#
#     students = school.create_vertex_collection(generate_col_name())
#     lectures = school.create_vertex_collection(generate_col_name())
#     enrolled = school.create_edge_definition(
#         name=generate_col_name(),
#         from_collections=[students.name],
#         to_collections=[lectures.name]
#     )
#     # Insert test vertices into the graph
#     students.insert({'_key': 'anna', 'name': 'Anna'})
#     students.insert({'_key': 'andy', 'name': 'Andy'})
#
#     lectures.insert({'_key': 'CSC101', 'name': 'Introduction to CS'})
#     lectures.insert({'_key': 'MAT223', 'name': 'Linear Algebra'})
#     lectures.insert({'_key': 'STA201', 'name': 'Statistics'})
#     lectures.insert({'_key': 'MAT101', 'name': 'Calculus I'})
#     lectures.insert({'_key': 'MAT102', 'name': 'Calculus II'})
#
#     # Insert test edges into the graph
#     enrolled.insert({
#         '_from': '{}/anna'.format(students.name),
#         '_to': '{}/CSC101'.format(lectures.name)
#     })
#     enrolled.insert({
#         '_from': '{}/anna'.format(students.name),
#         '_to': '{}/STA201'.format(lectures.name)
#     })
#     enrolled.insert({
#         '_from': '{}/anna'.format(students.name),
#         '_to': '{}/MAT223'.format(lectures.name)
#     })
#     enrolled.insert({
#         '_from': '{}/andy'.format(students.name),
#         '_to': '{}/MAT101'.format(lectures.name)
#     })
#     enrolled.insert({
#         '_from': '{}/andy'.format(students.name),
#         '_to': '{}/MAT102'.format(lectures.name)
#     })
#     enrolled.insert({
#         '_from': '{}/andy'.format(students.name),
#         '_to': '{}/MAT223'.format(lectures.name)
#     })
#
#     print(enrolled.edges('{}/anna'.format(students.name)))


def test_traverse(db):
    # Create test graph, vertex and edge collections
    school = db.create_graph(generate_graph_name())
    profs = school.create_vertex_collection(generate_col_name())
    classes = school.create_vertex_collection(generate_col_name())
    teaches = school.create_edge_definition(
        name=generate_col_name(),
        from_collections=[profs.name],
        to_collections=[classes.name]
    )
    # Insert test vertices into the graph
    profs.insert({'_key': 'anna', 'name': 'Professor Anna'})
    profs.insert({'_key': 'andy', 'name': 'Professor Andy'})
    classes.insert({'_key': 'CSC101', 'name': 'Introduction to CS'})
    classes.insert({'_key': 'MAT223', 'name': 'Linear Algebra'})
    classes.insert({'_key': 'STA201', 'name': 'Statistics'})
    classes.insert({'_key': 'MAT101', 'name': 'Calculus I'})
    classes.insert({'_key': 'MAT102', 'name': 'Calculus II'})

    # Insert test edges into the graph
    teaches.insert({
        '_from': '{}/anna'.format(profs.name),
        '_to': '{}/CSC101'.format(classes.name)
    })
    teaches.insert({
        '_from': '{}/anna'.format(profs.name),
        '_to': '{}/STA201'.format(classes.name)
    })
    teaches.insert({
        '_from': '{}/anna'.format(profs.name),
        '_to': '{}/MAT223'.format(classes.name)
    })
    teaches.insert({
        '_from': '{}/andy'.format(profs.name),
        '_to': '{}/MAT101'.format(classes.name)
    })
    teaches.insert({
        '_from': '{}/andy'.format(profs.name),
        '_to': '{}/MAT102'.format(classes.name)
    })
    teaches.insert({
        '_from': '{}/andy'.format(profs.name),
        '_to': '{}/MAT223'.format(classes.name)
    })

    # Traverse the graph with default settings
    result = school.traverse('{}/anna'.format(profs.name))
    visited = extract('_key', result['vertices'])
    assert visited == ['CSC101', 'MAT223', 'STA201', 'anna']

    for path in result['paths']:
        for vertex in path['vertices']:
            assert set(vertex) == {'_id', '_key', '_rev', 'name'}
        for edge in path['edges']:
            assert set(edge) == {'_id', '_key', '_rev', '_to', '_from'}

    result = school.traverse('{}/andy'.format(profs.name))
    visited = extract('_key', result['vertices'])
    assert visited == ['MAT101', 'MAT102', 'MAT223', 'andy']

    # Traverse the graph with an invalid start vertex
    with assert_raises(GraphTraverseError):
        school.traverse('invalid')

    with assert_raises(GraphTraverseError):
        bad_col_name = generate_col_name()
        school.traverse('{}/hanna'.format(bad_col_name))

    with assert_raises(GraphTraverseError):
        school.traverse('{}/anderson'.format(profs.name))

    # Travers the graph with max iteration of 0
    with assert_raises(GraphTraverseError):
        school.traverse('{}/andy'.format(profs.name), max_iter=0)

    # Traverse the graph with max depth of 0
    result = school.traverse('{}/andy'.format(profs.name), max_depth=0)
    assert extract('_key', result['vertices']) == ['andy']

    result = school.traverse('{}/anna'.format(profs.name), max_depth=0)
    assert extract('_key', result['vertices']) == ['anna']

    # Traverse the graph with min depth of 2
    result = school.traverse('{}/andy'.format(profs.name), min_depth=2)
    assert extract('_key', result['vertices']) == []

    result = school.traverse('{}/anna'.format(profs.name), min_depth=2)
    assert extract('_key', result['vertices']) == []

    # Traverse the graph with DFS and BFS
    result = school.traverse(
        '{}/anna'.format(profs.name),
        strategy='dfs',
        direction='any',
    )
    dfs_vertices = extract('_key', result['vertices'])

    result = school.traverse(
        '{}/anna'.format(profs.name),
        strategy='bfs',
        direction='any'
    )
    bfs_vertices = extract('_key', result['vertices'])

    assert sorted(dfs_vertices) == sorted(bfs_vertices)

    # Traverse the graph with filter function
    result = school.traverse(
        '{}/andy'.format(profs.name),
        filter_func='if (vertex._key == "MAT101") {return "exclude";} return;'
    )
    assert extract('_key', result['vertices']) == ['MAT102', 'MAT223', 'andy']

    # Traverse the graph with uniqueness (should be same as before)
    result = school.traverse(
        '{}/andy'.format(profs.name),
        vertex_uniqueness='global',
        edge_uniqueness='global',
        filter_func='if (vertex._key == "MAT101") {return "exclude";} return;'
    )
    assert extract('_key', result['vertices']) == ['MAT102', 'MAT223', 'andy']
