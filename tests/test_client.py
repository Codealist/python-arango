from __future__ import absolute_import, unicode_literals

import pytest
import requests

from arango.client import ArangoClient
from arango.database import Database
from arango.exceptions import ServerConnectionError
from arango.version import __version__
from tests.utils import (
    generate_database_name,
    generate_username,
    generate_string
)


def test_client_attributes():
    session = requests.Session()
    requests_kwargs = {'verify': False}

    client = ArangoClient(
        protocol='http',
        host='127.0.0.1',
        port=8529,
        session=session,
        request_kwargs=requests_kwargs
    )
    assert client.version == __version__
    assert client.protocol == 'http'
    assert client.host == '127.0.0.1'
    assert client.port == 8529
    assert client.session == session
    assert client.request_kwargs == requests_kwargs
    assert repr(client) == '<ArangoClient http://127.0.0.1:8529>'


def test_client_good_connection():
    session = requests.Session()
    requests_kwargs = {'verify': False}

    client = ArangoClient(
        protocol='http',
        host='127.0.0.1',
        port=8529,
        session=session,
        request_kwargs=requests_kwargs
    )

    # Test connection with verify flag on and off
    for verify in (True, False):
        db = client.db(verify=verify)
        assert isinstance(db, Database)
        assert db.name == '_system'
        assert db.username == 'root'
        assert db.context == 'default'
        assert db._conn._url_prefix == 'http://127.0.0.1:8529/_db/_system'
        assert db._conn._session == session
        assert db._conn._request_kwargs == requests_kwargs


def test_client_bad_connection():
    client = ArangoClient(protocol='http', host='127.0.0.1', port=8529)

    bad_db_name = generate_database_name()
    bad_username = generate_username()
    bad_password = generate_string()

    # Test connection with bad credentials
    with pytest.raises(ServerConnectionError) as err:
        client.db('_system', bad_username, bad_password)
    assert 'bad username and/or password' in str(err.value)

    # Test connection with missing database
    with pytest.raises(ServerConnectionError) as err:
        client.db(bad_db_name, bad_username, bad_password)
    assert 'database not found' in str(err.value)

    # Test connection with invalid host URL
    client._url = 'http://127.0.0.1:8500'
    with pytest.raises(ServerConnectionError) as err:
        client.db('_system', 'root', '')
    assert 'bad connection' in str(err.value)
