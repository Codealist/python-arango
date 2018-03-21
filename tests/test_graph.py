from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.collection import EdgeCollection
from arango.exceptions import (
    DocumentParseError,
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
    OrphanCollectionListError,
    VertexCollectionCreateError,
    VertexCollectionDeleteError,
    VertexCollectionListError,
    GraphListError,
    GraphCreateError,
    GraphDeleteError,
    GraphPropertiesError,
    GraphTraverseError,
)
from tests.utils import (
    generate_collection_name,
    generate_graph_name,
    generate_document_key,
    clean,
    extract
)


def test_graph_properties(graph, bad_graph, db):
    assert repr(graph) == '<Graph {}>'.format(graph.name)

    properties = graph.properties()
    assert properties['id'] == '_graphs/{}'.format(graph.name)
    assert properties['name'] == graph.name
    assert len(properties['edge_definitions']) == 1
    assert len(properties['orphan_collections']) == 2
    assert properties['smart'] is False
    assert 'smart_field' in properties
    assert 'shard_count' in properties
    assert isinstance(properties['revision'], string_types)

    # Test properties with bad credentials
    with pytest.raises(GraphPropertiesError):
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
    # assert properties['smart'] == True
    # assert properties['smart_field'] == 'foo'
    # assert properties['shard_count'] == 2


def test_graph_management(db, bad_db):
    # Test create graph
    graph_name = generate_graph_name()
    graph = db.create_graph(graph_name)
    assert graph.name == graph_name
    assert graph.database == db.name

    # Test create duplicate graph
    with pytest.raises(GraphCreateError) as err:
        db.create_graph(graph_name)
    assert 'already exists' in str(err.value)

    # Test get graph
    result = db.graph(graph_name)
    assert result.name == graph.name
    assert result.database == graph.database

    # Test get graphs
    result = db.graphs()
    for entry in result:
        assert 'revision' in entry
        assert 'edge_definitions' in entry
        assert 'orphan_collections' in entry
    assert graph_name in extract('name', db.graphs())

    # Test get graphs with bad credentials
    with pytest.raises(GraphListError):
        bad_db.graphs()

    # Test delete graph
    assert db.delete_graph(graph_name) is True
    assert graph_name not in extract('name', db.graphs())

    # Test delete missing graph
    with pytest.raises(GraphDeleteError) as err:
        db.delete_graph(graph_name)
    assert 'not found' in str(err.value)
    assert db.delete_graph(graph_name, ignore_missing=True) is False

    # Create a graph with vertex and edge collections and delete the graph
    graph = db.create_graph(graph_name)
    ecol_name = generate_collection_name()
    fvcol_name = generate_collection_name()
    tvcol_name = generate_collection_name()

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
    fvcol_name = generate_collection_name()
    fvcol = graph.create_vertex_collection(fvcol_name)
    assert fvcol.name == fvcol_name
    assert fvcol.graph == graph.name
    assert fvcol_name in repr(fvcol)
    assert fvcol_name in graph.vertex_collections()
    assert fvcol_name in graph.orphan_collections()
    assert fvcol_name in extract('name', db.collections())

    # Test create duplicate vertex collection
    with pytest.raises(VertexCollectionCreateError) as err:
        graph.create_vertex_collection(fvcol_name)
    assert 'collection used' in str(err.value)
    assert fvcol_name in graph.vertex_collections()
    assert fvcol_name in graph.orphan_collections()
    assert fvcol_name in extract('name', db.collections())

    # Test create valid to vertex collection
    tvcol_name = generate_collection_name()
    tvcol = graph.create_vertex_collection(tvcol_name)
    assert tvcol_name == tvcol_name
    assert tvcol.graph == graph.name
    assert tvcol_name in repr(tvcol)
    assert tvcol_name in graph.vertex_collections()
    assert tvcol_name in graph.orphan_collections()
    assert tvcol_name in extract('name', db.collections())

    # Test list vertex collection via bad database
    with pytest.raises(VertexCollectionListError):
        bad_graph.vertex_collections()

    # Test list orphan collection via bad database
    with pytest.raises(OrphanCollectionListError):
        bad_graph.orphan_collections()

    # Test delete missing vertex collection
    with pytest.raises(VertexCollectionDeleteError) as err:
        graph.delete_vertex_collection(generate_collection_name())
    assert err.value.http_code == 404

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
    ecol_name = generate_collection_name()
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
    with pytest.raises(EdgeDefinitionCreateError) as err:
        graph.create_edge_definition(ecol_name, [], [])
    assert 'multi use' in str(err.value)

    # Test create edge definition with existing vertex collection
    fvcol_name = generate_collection_name()
    tvcol_name = generate_collection_name()
    graph.create_vertex_collection(fvcol_name)
    graph.create_vertex_collection(tvcol_name)
    ecol_name = generate_collection_name()
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
    bad_vcol_name = generate_collection_name()
    ecol_name = generate_collection_name()
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

    # Test list edge definition with bad credentials
    with pytest.raises(EdgeDefinitionListError):
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
    bad_ecol_name = generate_collection_name()
    with pytest.raises(EdgeDefinitionReplaceError):
        graph.replace_edge_definition(
            name=bad_ecol_name,
            from_collections=[],
            to_collections=[fvcol_name]
        )

    # Test delete missing edge definition
    with pytest.raises(EdgeDefinitionDeleteError) as err:
        graph.delete_edge_definition(bad_ecol_name)
    assert err.value.http_code == 404

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
    new_ecol_name = generate_collection_name()
    fvcol_name = generate_collection_name()
    tvcol_name = generate_collection_name()
    ovcol_name = generate_collection_name()

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
    vertex = fvdocs[1]
    key = vertex['_key']

    # Test insert first valid vertex
    result = fvcol.insert(vertex)
    assert result['_key'] == key
    assert '_rev' in result
    assert vertex in fvcol and key in fvcol
    assert len(fvcol) == 1
    assert fvcol[key]['val'] == vertex['val']

    # Test insert duplicate vertex
    with pytest.raises(DocumentInsertError) as err:
        fvcol.insert(vertex)
    assert 'unique constraint violated' in str(err.value)
    assert len(fvcol) == 1

    vertex = fvdocs[0]
    key = vertex['_key']

    # Test insert second valid vertex
    result = fvcol.insert(vertex, sync=True)
    assert result['_key'] == key
    assert vertex in fvcol and key in fvcol
    assert len(fvcol) == 2
    assert fvcol[key]['val'] == vertex['val']

    # Test get missing vertex
    bad_document_key = generate_document_key()
    assert fvcol.get(bad_document_key) is None

    # Test get existing vertex by key
    result = fvcol.get(key)
    assert clean(result) == vertex

    # Test get existing vertex by ID
    result = fvcol.get('{}/{}'.format(fvcol.name, key))
    assert clean(result) == vertex

    # Test get existing vertex by malformed ID
    with pytest.raises(DocumentParseError) as err:
        fvcol.get('{}/{}'.format(generate_collection_name(), key))
    assert 'bad collection name' in str(err.value)

    # Test get existing vertex with wrong revision
    old_rev = result['_rev']
    with pytest.raises(DocumentRevisionError) as err:
        fvcol.get(key, rev=old_rev + '1')
    assert 'wrong revision' in str(err.value)

    # Test get existing vertex with bad credentials
    with pytest.raises(DocumentGetError):
        bad_fvcol.get(key)

    # Test update vertex with a single field change
    assert 'foo' not in fvcol.get(key)
    result = fvcol.update({'_key': key, 'foo': 100})
    assert result['_key'] == key
    assert fvcol[key]['foo'] == 100
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

    # Test update vertex with incorrect revision
    new_rev = old_rev + '1'
    with pytest.raises(DocumentRevisionError):
        fvcol.update({'_key': key, '_rev': new_rev, 'bar': 500})
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400

    # Test update vertex in missing vertex collection
    with pytest.raises(DocumentUpdateError):
        bad_fvcol.update({'_key': key, 'bar': 500})
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400

    # Test update vertex with sync option
    result = fvcol.update({'_key': key, 'bar': 500}, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 500
    old_rev = result['_rev']

    # Test update vertex with keep_none option
    result = fvcol.update({'_key': key, 'bar': None}, keep_none=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] is None
    old_rev = result['_rev']

    # Test update vertex without keep_none option
    result = fvcol.update({'_key': key, 'foo': None}, keep_none=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert 'foo' not in fvcol[key]
    assert fvcol['1']['bar'] is None

    # Test replace vertex with a single field change
    result = fvcol.replace({'_key': key, 'baz': 100})
    assert result['_key'] == key
    assert 'foo' not in fvcol[key]
    assert 'bar' not in fvcol[key]
    assert fvcol[key]['baz'] == 100
    old_rev = result['_rev']

    # Test replace vertex with multiple field changes
    vertex = {'_key': key, 'foo': 200, 'bar': 300}
    result = fvcol.replace(vertex)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert clean(fvcol[key]) == vertex
    old_rev = result['_rev']

    # Test replace vertex with correct revision
    vertex = {'_key': key, '_rev': old_rev, 'bar': 500}
    result = fvcol.replace(vertex)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert clean(fvcol[key]) == clean(vertex)
    old_rev = result['_rev']

    # Test replace vertex with incorrect revision
    new_rev = old_rev + '10'
    vertex = {'_key': key, '_rev': new_rev, 'bar': 600}
    with pytest.raises(DocumentRevisionError) as err:
        fvcol.replace(vertex)
    assert 'wrong revision' in str(err.value)
    assert fvcol[key]['bar'] == 500
    assert 'foo' not in fvcol[key]

    # Test replace vertex with bad credentials
    with pytest.raises(DocumentReplaceError):
        bad_fvcol.replace({'_key': key, 'bar': 600})
    assert fvcol[key]['bar'] == 500
    assert 'foo' not in fvcol[key]

    # Test replace vertex with sync option
    vertex = {'_key': key, 'bar': 400, 'foo': 200}
    result = fvcol.replace(vertex, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert fvcol[key]['foo'] == 200
    assert fvcol[key]['bar'] == 400

    # Test delete vertex with incorrect revision
    old_rev = fvcol[key]['_rev']
    vertex['_rev'] = old_rev + '1'
    with pytest.raises(DocumentRevisionError) as err:
        fvcol.delete(vertex, check_rev=True)
    assert 'wrong revision' in str(err.value)
    vertex['_rev'] = old_rev
    assert vertex in fvcol

    # Test delete vertex with bad credentials
    with pytest.raises(DocumentDeleteError):
        bad_fvcol.delete(vertex, ignore_missing=False)

    # Test delete missing vertex
    with pytest.raises(DocumentDeleteError) as err:
        fvcol.delete(bad_document_key, ignore_missing=False)
    assert err.value.http_code == 404
    assert fvcol.delete(bad_document_key, ignore_missing=True) is False

    # Test delete existing vertex with sync
    assert fvcol.delete(vertex, sync=True, check_rev=False) is True
    assert fvcol[vertex] is None
    assert vertex not in fvcol


def test_edge_management(ecol, bad_ecol, edocs, fvcol, fvdocs, tvcol, tvdocs):
    for vertex in fvdocs:
        fvcol.insert(vertex)
    for vertex in tvdocs:
        tvcol.insert(vertex)

    edge = edocs[0]
    key = edge['_key']

    # Test insert first valid edge
    result = ecol.insert(edge)
    assert result['_key'] == key
    assert '_rev' in result
    assert edge in ecol and key in ecol
    assert len(ecol) == 1
    assert ecol[key]['_from'] == edge['_from']
    assert ecol[key]['_to'] == edge['_to']

    # Test insert duplicate edge
    with pytest.raises(DocumentInsertError) as err:
        assert ecol.insert(edge)
    assert 'unique constraint violated' in str(err.value)
    assert len(ecol) == 1

    edge = edocs[1]
    key = edge['_key']

    # Test insert second valid edge
    result = ecol.insert(edge, sync=True)
    assert result['_key'] == key
    assert edge in ecol and key in ecol
    assert len(ecol) == 2
    assert ecol[key]['_from'] == edge['_from']
    assert ecol[key]['_to'] == edge['_to']

    # Test get missing vertex
    bad_document_key = generate_document_key()
    assert ecol.get(bad_document_key) is None

    # Test get existing edge by key
    result = ecol.get(key)
    assert clean(result) == edge

    # Test get existing edge by ID
    result = ecol.get('{}/{}'.format(ecol.name, key))
    assert clean(result) == edge

    # Test get existing edge by malformed ID
    with pytest.raises(DocumentParseError) as err:
        ecol.get('{}/{}'.format(generate_collection_name(), key))
    assert 'bad collection name' in str(err.value)

    # Test get existing edge with wrong revision
    old_rev = result['_rev']
    with pytest.raises(DocumentRevisionError) as err:
        ecol.get(key, rev=old_rev + '1')
    assert 'wrong revision' in str(err.value)

    # Test get existing vertex with bad credentials
    with pytest.raises(DocumentGetError):
        bad_ecol.get(key)

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

    # Test update edge with incorrect revision
    new_rev = old_rev + '1'
    with pytest.raises(DocumentRevisionError):
        ecol.update({'_key': key, '_rev': new_rev, 'bar': 500})
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 400

    # Test update edge in missing edge collection
    with pytest.raises(DocumentUpdateError):
        bad_ecol.update({'_key': key, 'bar': 500})
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 400

    # Test update edge with sync option
    result = ecol.update({'_key': key, 'bar': 500}, sync=True)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 200
    assert ecol[key]['bar'] == 500
    old_rev = result['_rev']

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

    # Test replace edge with incorrect revision
    edge['bar'] = 500
    edge['_rev'] = old_rev + key
    with pytest.raises(DocumentRevisionError) as err:
        ecol.replace(edge)
    assert 'wrong revision' in str(err.value)
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 400

    # Test replace edge with bad credentials
    with pytest.raises(DocumentReplaceError):
        bad_ecol.replace(edge)
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 400

    # Test replace edge with sync option
    result = ecol.replace(edge, sync=True, check_rev=False)
    assert result['_key'] == key
    assert result['_old_rev'] == old_rev
    assert ecol[key]['foo'] == 300
    assert ecol[key]['bar'] == 500

    # Test delete edge with incorrect revision
    old_rev = ecol[key]['_rev']
    edge['_rev'] = old_rev + '1'
    with pytest.raises(DocumentRevisionError):
        ecol.delete(edge, check_rev=True)
    assert 'wrong revision' in str(err.value)
    edge['_rev'] = old_rev
    assert edge in ecol

    # Test delete vertex with bad credentials
    with pytest.raises(DocumentDeleteError):
        bad_ecol.delete(edge, ignore_missing=False)

    # Test delete missing edge
    with pytest.raises(DocumentDeleteError) as err:
        ecol.delete(bad_document_key, ignore_missing=False)
    assert err.value.http_code == 404
    assert not ecol.delete(bad_document_key, ignore_missing=True)

    # Test delete existing edge with sync
    assert ecol.delete(edge, sync=True, check_rev=False) is True
    assert ecol[edge] is None
    assert edge not in ecol


# TODO ArangoDB 3.3.4 is throwing 501 ILLEGAL /_api/edges' not implemented
# def test_vertex_edges(db):
#     # Create test graph, vertex and edge collections
#     school = db.create_graph('school')
#
#     students = school.create_vertex_collection('students')
#     lectures = school.create_vertex_collection('lectures')
#     enrolled = school.create_edge_definition(
#         name='enrolled',
#         from_collections=['students'],
#         to_collections=['lectures']
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
#     enrolled.insert({'_from': 'students/anna', '_to': 'lectures/CSC101'})
#     enrolled.insert({'_from': 'students/anna', '_to': 'lectures/STA201'})
#     enrolled.insert({'_from': 'students/anna', '_to': 'lectures/MAT223'})
#     enrolled.insert({'_from': 'students/andy', '_to': 'lectures/MAT101'})
#     enrolled.insert({'_from': 'students/andy', '_to': 'lectures/MAT102'})
#     enrolled.insert({'_from': 'students/andy', '_to': 'lectures/MAT223'})


def test_traverse(db):
    # Create test graph, vertex and edge collections
    curriculum = db.create_graph('curriculum')
    professors = curriculum.create_vertex_collection('profs')
    classes = curriculum.create_vertex_collection('classes')
    teaches = curriculum.create_edge_definition(
        name='teaches',
        from_collections=['profs'],
        to_collections=['classes']
    )
    # Insert test vertices into the graph
    professors.insert({'_key': 'anna', 'name': 'Professor Anna'})
    professors.insert({'_key': 'andy', 'name': 'Professor Andy'})
    classes.insert({'_key': 'CSC101', 'name': 'Introduction to CS'})
    classes.insert({'_key': 'MAT223', 'name': 'Linear Algebra'})
    classes.insert({'_key': 'STA201', 'name': 'Statistics'})
    classes.insert({'_key': 'MAT101', 'name': 'Calculus I'})
    classes.insert({'_key': 'MAT102', 'name': 'Calculus II'})

    # Insert test edges into the graph
    teaches.insert({'_from': 'profs/anna', '_to': 'classes/CSC101'})
    teaches.insert({'_from': 'profs/anna', '_to': 'classes/STA201'})
    teaches.insert({'_from': 'profs/anna', '_to': 'classes/MAT223'})
    teaches.insert({'_from': 'profs/andy', '_to': 'classes/MAT101'})
    teaches.insert({'_from': 'profs/andy', '_to': 'classes/MAT102'})
    teaches.insert({'_from': 'profs/andy', '_to': 'classes/MAT223'})

    # Traverse the graph with default settings
    result = curriculum.traverse(start_vertex='profs/anna')
    visited = extract('_key', result['vertices'])
    assert visited == ['CSC101', 'MAT223', 'STA201', 'anna']

    for path in result['paths']:
        for vertex in path['vertices']:
            assert set(vertex) == {'_id', '_key', '_rev', 'name'}
        for edge in path['edges']:
            assert set(edge) == {'_id', '_key', '_rev', '_to', '_from'}

    result = curriculum.traverse(start_vertex='profs/andy')
    visited = sorted([v['_key'] for v in result['vertices']])
    assert visited == ['MAT101', 'MAT102', 'MAT223', 'andy']

    # Traverse the graph with an invalid start vertex
    with pytest.raises(GraphTraverseError):
        curriculum.traverse(start_vertex='invalid')

    with pytest.raises(GraphTraverseError):
        curriculum.traverse(start_vertex='students/hanna')

    with pytest.raises(GraphTraverseError):
        curriculum.traverse(start_vertex='profs/anderson')

    # Travers the graph with max iteration of 0
    with pytest.raises(GraphTraverseError):
        curriculum.traverse(start_vertex='profs/andy', max_iter=0)

    # Traverse the graph with max depth of 0
    result = curriculum.traverse(start_vertex='profs/andy', max_depth=0)
    assert extract('_key', result['vertices']) == ['andy']

    result = curriculum.traverse(start_vertex='profs/anna', max_depth=0)
    assert extract('_key', result['vertices']) == ['anna']

    # Traverse the graph with min depth of 2
    result = curriculum.traverse(start_vertex='profs/andy', min_depth=2)
    assert extract('_key', result['vertices']) == []

    result = curriculum.traverse(start_vertex='profs/anna', min_depth=2)
    assert extract('_key', result['vertices']) == []

    # Traverse the graph with DFS and BFS
    result = curriculum.traverse(
        start_vertex='profs/anna',
        strategy='dfs',
        direction='any',
    )
    dfs_vertices = extract('_key', result['vertices'])

    result = curriculum.traverse(
        start_vertex='profs/anna',
        strategy='bfs',
        direction='any'
    )
    bfs_vertices = extract('_key', result['vertices'])

    assert sorted(dfs_vertices) == sorted(bfs_vertices)

    # Traverse the graph with filter function
    result = curriculum.traverse(
        start_vertex='profs/andy',
        filter_func='if (vertex._key == "MAT101") {return "exclude";} return;'
    )
    assert extract('_key', result['vertices']) == ['MAT102', 'MAT223', 'andy']

    # Traverse the graph with uniqueness (should be same as before)
    result = curriculum.traverse(
        start_vertex='profs/andy',
        vertex_uniqueness='global',
        edge_uniqueness='global',
        filter_func='if (vertex._key == "MAT101") {return "exclude";} return;'
    )
    assert extract('_key', result['vertices']) == ['MAT102', 'MAT223', 'andy']
