from __future__ import absolute_import, unicode_literals, division

import pytest

from arango import ArangoClient
from tests.utils import (
    generate_database_name,
    generate_collection_name,
    generate_string,
    generate_username,
    generate_graph_name,
    purge_test_databases,
    purge_test_tasks,
    clean_test_users,
    purge_test_collections
)

print('Setting up test client ...')
_client = ArangoClient()
_sys_db = _client.db('_system')

print('Setting up test databases ...')
_db_name = generate_database_name()
_username = generate_username()
_password = generate_string()
_db_users = [{
    'username': _username,
    'password': _password,
    'active': True
}]
_sys_db.create_database(_db_name, _db_users)
_db = _client.db(_db_name, _username, _password)
_bad_db_name = generate_database_name()
_bad_db = _client.db(_bad_db_name, '', '', verify=False)

print('Setting up test collections ...')
_col_name = generate_collection_name()
_col = _db.create_collection(_col_name)
_col.add_fulltext_index(['val'])
_col.add_geo_index(['loc'])
_bad_col = _bad_db.collection(_col_name)
_lecol_name = generate_collection_name()
_lecol = _db.create_collection(_lecol_name, edge=True)

print('Setting up test graphs ...')
_graph_name = generate_graph_name()
_graph = _db.create_graph(_graph_name)
_bad_graph = _bad_db.graph(_graph_name)

print('Setting up test "_from" vertex collections ...')
_fvcol_name = generate_collection_name()
_fvcol = _graph.create_vertex_collection(_fvcol_name)
_bad_fvcol = _bad_graph.vertex_collection(_fvcol_name)

print('Setting up test "_to" vertex collections ...')
_tvcol_name = generate_collection_name()
_tvcol = _graph.create_vertex_collection(_tvcol_name)
_bad_tvcol = _bad_graph.vertex_collection(_tvcol_name)

print('Setting up test edge collection and definition ...')
_ecol_name = generate_collection_name()
_ecol = _graph.create_edge_definition(
    name=_ecol_name,
    from_collections=[_fvcol_name],
    to_collections=[_tvcol_name]
)
_bad_ecol = _bad_graph.vertex_collection(_ecol_name)

print('Setting up test documents ...')
_docs = [
    {'_key': '1', 'val': 1, 'text': 'foo', 'loc': [1, 1]},
    {'_key': '2', 'val': 2, 'text': 'foo', 'loc': [2, 2]},
    {'_key': '3', 'val': 3, 'text': 'foo', 'loc': [3, 3]},
    {'_key': '4', 'val': 4, 'text': 'bar', 'loc': [4, 4]},
    {'_key': '5', 'val': 5, 'text': 'bar', 'loc': [5, 5]},
    {'_key': '6', 'val': 6, 'text': 'bar', 'loc': [5, 5]},
]
print('Setting up test "_from" vertex documents ...')
_fvdocs = [
    {'_key': '1', 'val': 1},
    {'_key': '2', 'val': 2},
    {'_key': '3', 'val': 3},
]
print('Setting up test "_to" vertex documents ...')
_tvdocs = [
    {'_key': '4', 'val': 4},
    {'_key': '5', 'val': 5},
    {'_key': '6', 'val': 6},
]
print('Setting up test edge documents ...')
_edocs = [
    {
        '_key': '1',
        '_from': '{}/1'.format(_fvcol_name),
        '_to': '{}/4'.format(_tvcol_name)
    },
    {
        '_key': '2',
        '_from': '{}/1'.format(_fvcol_name),
        '_to': '{}/5'.format(_tvcol_name)
    },
    {
        '_key': '3',
        '_from': '{}/6'.format(_fvcol_name),
        '_to': '{}/2'.format(_tvcol_name)
    },
    {
        '_key': '4',
        '_from': '{}/8'.format(_fvcol_name),
        '_to': '{}/7'.format(_tvcol_name)
    },
]


@pytest.fixture(autouse=False)
def client():
    return _client


@pytest.fixture(autouse=False)
def sys_db():
    return _sys_db


@pytest.fixture(autouse=False)
def db():
    return _db


@pytest.fixture(autouse=False)
def bad_db():
    return _bad_db


@pytest.fixture(autouse=False)
def col():
    _col.truncate()
    return _col


@pytest.fixture(autouse=False)
def lecol():
    _lecol.truncate()
    return _lecol


@pytest.fixture(autouse=False)
def bad_col():
    return _bad_col


@pytest.fixture(autouse=False)
def graph():
    return _graph


@pytest.fixture(autouse=False)
def bad_graph():
    return _bad_graph


@pytest.fixture(autouse=False)
def fvcol():
    _fvcol.truncate()
    return _fvcol


@pytest.fixture(autouse=False)
def bad_fvcol():
    return _bad_fvcol


@pytest.fixture(autouse=False)
def tvcol():
    _tvcol.truncate()
    return _tvcol


@pytest.fixture(autouse=False)
def bad_tvcol():
    return _bad_tvcol


@pytest.fixture(autouse=False)
def ecol():
    return _ecol


@pytest.fixture(autouse=False)
def bad_ecol():
    return _bad_ecol


@pytest.fixture(autouse=False)
def bad_graph():
    return _bad_graph


@pytest.fixture(autouse=False)
def docs():
    return [doc.copy() for doc in _docs]


@pytest.fixture(autouse=False)
def fvdocs():
    return [doc.copy() for doc in _fvdocs]


@pytest.fixture(autouse=False)
def tvdocs():
    return [doc.copy() for doc in _tvdocs]


@pytest.fixture(autouse=False)
def edocs():
    return [doc.copy() for doc in _edocs]


@pytest.fixture(autouse=False)
def username():
    return _username


def pytest_unconfigure(*_):
    _sys_db.clear_async_jobs()
    clean_test_users(_sys_db)
    purge_test_tasks(_sys_db)
    purge_test_databases(_sys_db)
    purge_test_collections(_sys_db)
