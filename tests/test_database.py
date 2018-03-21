from __future__ import absolute_import, unicode_literals

from datetime import datetime

import pytest
from six import string_types

from arango.collection import Collection
from arango.exceptions import (
    CollectionCreateError,
    CollectionDeleteError,
    CollectionListError,
    DatabasePropertiesError,
    ServerDetailsError,
    ServerEchoError,
    ServerLogLevelError,
    ServerLogLevelSetError,
    ServerReadLogError,
    ServerReloadRoutingError,
    ServerTargetVersionError,
    ServerRoleError,
    ServerStatisticsError,
    ServerTimeError,
    ServerVersionError,
    ServerEndpointsError,
    DatabaseListError,
    DatabaseCreateError,
    DatabaseDeleteError
)
from tests.utils import (
    generate_database_name,
    generate_collection_name,
    extract)


def test_database_properties(db, bad_db, username):
    assert repr(db) == '<Database {}>'.format(db.name)
    assert db.username == username

    # Test get database properties
    properties = db.properties()
    assert 'id' in properties
    assert 'path' in properties
    assert properties['name'] == db.name
    assert properties['system'] is False

    # Test get database properties with bad credentials
    with pytest.raises(DatabasePropertiesError):
        bad_db.properties()

    # Test get server version
    assert isinstance(db.version(), string_types)

    # Test get server version with bad credentials
    with pytest.raises(ServerVersionError):
        bad_db.version()

    # Test get server details
    details = db.details()
    assert 'architecture' in details
    assert 'server-version' in details

    # Test get server details with bad credentials
    with pytest.raises(ServerDetailsError):
        bad_db.details()

    # Test get server target version
    version = db.target_version()
    assert isinstance(version, string_types)

    # Test get server target version with bad credentials
    with pytest.raises(ServerTargetVersionError):
        bad_db.target_version()

    # Test get server statistics
    statistics = db.statistics(description=False)
    assert isinstance(statistics, dict)
    assert 'time' in statistics
    assert 'system' in statistics
    assert 'server' in statistics

    # Test get server statistics with description
    description = db.statistics(description=True)
    assert isinstance(description, dict)
    assert 'figures' in description
    assert 'groups' in description

    # Test get server statistics with bad credentials
    with pytest.raises(ServerStatisticsError):
        bad_db.statistics()

    # Test get server role
    assert db.role() in {
        'SINGLE',
        'COORDINATOR',
        'PRIMARY',
        'SECONDARY',
        'UNDEFINED'
    }

    # Test get server role with bad credentials
    with pytest.raises(ServerRoleError):
        bad_db.role()

    # Test get server time
    assert isinstance(db.time(), datetime)

    # Test get server time with bad credentials
    with pytest.raises(ServerTimeError):
        bad_db.time()

    # Test echo (get last request)
    last_request = db.echo()
    assert 'protocol' in last_request
    assert 'user' in last_request
    assert 'requestType' in last_request
    assert 'rawRequestBody' in last_request

    # Test echo with bad credentials
    with pytest.raises(ServerEchoError):
        bad_db.echo()

    # Test read_log with default arguments
    log = db.read_log(upto='fatal')
    assert 'lid' in log
    assert 'level' in log
    assert 'text' in log
    assert 'total_amount' in log

    # Test read_log with specific arguments
    log = db.read_log(
        level='error',
        start=0,
        size=100000,
        offset=0,
        search='test',
        sort='desc',
    )
    assert 'lid' in log
    assert 'level' in log
    assert 'text' in log
    assert 'total_amount' in log

    # Test read_log with bad credentials
    with pytest.raises(ServerReadLogError):
        bad_db.read_log()

    # Test reload routing
    assert isinstance(db.reload_routing(), bool)

    # Test reload routing with bad credentials
    with pytest.raises(ServerReloadRoutingError):
        bad_db.reload_routing()

    # Test get log levels
    assert isinstance(db.log_levels(), dict)

    # Test get log levels with bad credentials
    with pytest.raises(ServerLogLevelError):
        bad_db.log_levels()

    # Test set log levels
    new_levels = {
        'agency': 'DEBUG',
        'collector': 'INFO',
        'threads': 'WARNING'
    }
    result = db.set_log_levels(**new_levels)

    for key, value in new_levels.items():
        assert result[key] == value

    for key, value in db.log_levels().items():
        assert result[key] == value

    # Test set log levels with bad credentials
    with pytest.raises(ServerLogLevelSetError):
        bad_db.set_log_levels(**new_levels)

    # Test get server endpoints
    with pytest.raises(ServerEndpointsError) as err:
        db.endpoints()
    assert err.value.http_code == 403

    # Test get server endpoints with bad credentials
    with pytest.raises(ServerEndpointsError):
        bad_db.endpoints()


def test_database_management(db, sys_db, bad_db):
    # Test list databases
    result = db.databases()
    assert '_system' in result

    # Test list databases with bad credentials
    with pytest.raises(DatabaseListError):
        bad_db.databases()

    # Test create database
    db_name = generate_database_name()
    assert sys_db.create_database(db_name) is True
    assert db_name in sys_db.databases()

    # Test create duplicate database
    with pytest.raises(DatabaseCreateError) as err:
        sys_db.create_database(db_name)
    assert 'duplicate' in str(err.value)

    # Test create database without permissions
    with pytest.raises(DatabaseCreateError) as err:
        db.create_database(db_name)
    assert err.value.http_code == 403

    # Test delete database with bad credentials
    with pytest.raises(DatabaseDeleteError) as err:
        db.delete_database(db_name)
    assert err.value.http_code == 403

    # Test delete database
    assert sys_db.delete_database(db_name) is True
    assert db_name not in sys_db.databases()

    # Test delete missing database
    with pytest.raises(DatabaseDeleteError) as err:
        sys_db.delete_database(db_name)
    assert err.value.http_code == 404
    assert sys_db.delete_database(db_name, ignore_missing=True) is False


def test_collection_management(db, bad_db):
    # Test create collection
    col_name = generate_collection_name()
    col = db.create_collection(
        name=col_name,
        sync=True,
        compact=False,
        journal_size=7774208,
        system=False,
        volatile=False,
        key_generator='autoincrement',
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_fields=['test_attr'],
        index_bucket_count=10,
        replication_factor=1
    )
    properties = col.properties()
    assert 'id' in properties
    assert properties['name'] == col_name
    assert properties['sync'] is True
    assert properties['system'] is False
    assert properties['edge'] is True
    assert properties['key_generator'] == 'autoincrement'
    assert properties['user_keys'] is False
    assert properties['key_increment'] == 9
    assert properties['key_offset'] == 100

    # Test create duplicate collection
    with pytest.raises(CollectionCreateError) as err:
        db.create_collection(col_name)
    assert 'duplicate' in str(err.value)

    # Test list collections
    assert all(
        entry['name'].startswith('test_collection')
        or entry['name'].startswith('_')
        for entry in db.collections()
    )

    # Test list collections with bad credentials
    with pytest.raises(CollectionListError):
        bad_db.collections()

    # Test get collection object
    test_col = db.collection(col.name)
    assert isinstance(test_col, Collection)
    assert test_col.name == col.name

    test_col = db[col.name]
    assert isinstance(test_col, Collection)
    assert test_col.name == col.name

    # Test delete collection
    assert db.delete_collection(col_name) is True
    assert col_name not in extract('name', db.collections())

    # Test drop missing collection
    with pytest.raises(CollectionDeleteError) as err:
        db.delete_collection(col_name)
    assert err.value.http_code == 404
    assert db.delete_collection(col_name, ignore_missing=True) is False
