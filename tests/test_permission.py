import pytest

from arango.exceptions import (
    CollectionCreateError,
    CollectionListError,
    PermissionDeleteError,
    PermissionGetError,
    PermissionUpdateError,
)
from tests.utils import (
    generate_username,
    generate_collection_name,
    generate_string,
    generate_database_name,
    extract
)

def test_permission_management(client, sys_db, bad_db):
    username = generate_username()
    password = generate_string()
    db_name = generate_database_name()
    col_name_1 = generate_collection_name()
    col_name_2 = generate_collection_name()

    sys_db.create_database(
        name=db_name,
        users=[{
            'username': username,
            'password': password,
            'active': True
        }]
    )
    db = client.db(db_name, username, password)
    assert isinstance(sys_db.permissions(username), dict)

    # Test permissions get with bad credentials
    with pytest.raises(PermissionGetError):
        bad_db.permissions(username)

    # Test permission get with bad credentials
    with pytest.raises(PermissionGetError):
        bad_db.permission(username, db_name)

    # The user should have read and write permissions
    assert sys_db.permission(username, db_name) == 'rw'
    db.create_collection(col_name_1)
    assert col_name_1 in extract('name', db.collections())
    assert sys_db.permission(username, db_name, col_name_1) == 'rw'

    # Update user permission to none and try again
    assert sys_db.update_permission(username, 'none', db_name) is True
    assert sys_db.permission(username, db_name) == 'none'

    with pytest.raises(CollectionCreateError) as err:
        db.create_collection(col_name_1)
    assert err.value.http_code in {401, 403}

    with pytest.raises(CollectionListError) as err:
        db.collections()
    assert err.value.http_code in {401, 403}

    # Test permission update with bad credentials
    with pytest.raises(PermissionUpdateError):
        bad_db.update_permission(username, 'ro', db_name)
    assert sys_db.permission(username, db_name) == 'none'

    # Update user permission to read only and try again
    assert sys_db.update_permission(username, 'ro', db_name) is True
    assert sys_db.permission(username, db_name) == 'ro'

    with pytest.raises(CollectionCreateError) as err:
        db.create_collection(col_name_2)
    assert err.value.http_code in {401, 403}

    assert col_name_1 in extract('name', db.collections())
    assert col_name_2 not in extract('name', db.collections())

    # Test permission delete with bad credentials
    with pytest.raises(PermissionDeleteError):
        bad_db.delete_permission(username, '', db_name)
    assert sys_db.permission(username, db_name) == 'ro'

    # Delete user permission and try again
    assert sys_db.delete_permission(username, db_name) is True
    assert sys_db.permission(username, db_name) == 'none'

    with pytest.raises(CollectionCreateError) as err:
        db.create_collection(col_name_1)
    assert err.value.http_code in {401, 403}

    with pytest.raises(CollectionListError) as err:
        db.collections()
    assert err.value.http_code in {401, 403}
