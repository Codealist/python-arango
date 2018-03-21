from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.exceptions import (
    DatabasePropertiesError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserReplaceError,
    UserUpdateError,
)
from tests.utils import (
    generate_username,
    generate_database_name,
    generate_string,
    extract)


def test_user_management(sys_db, bad_db):
    username = generate_username()
    password = generate_string()
    new_user = sys_db.create_user(
        username=username,
        password=password,
        active=True,
        extra={'foo': 'bar'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is True
    assert new_user['extra'] == {'foo': 'bar'}

    # Create a duplicate user
    with pytest.raises(UserCreateError) as err:
        sys_db.create_user(
            username=username,
            password=password
        )
    assert 'duplicate' in str(err.value)

    # Test get users
    for user in sys_db.users():
        assert isinstance(user['username'], string_types)
        assert isinstance(user['active'], bool)
        assert isinstance(user['extra'], dict)
    assert sys_db.user(username) == new_user

    # Test get users with bad credentials
    with pytest.raises(UserListError):
        bad_db.users()

    # Test get user
    assert username in extract('username', sys_db.users())

    # Test get user with bad credentials
    with pytest.raises(UserGetError) as err:
        sys_db.user(generate_username())
    assert err.value.http_code == 404

    # Update an existing user
    new_user = sys_db.update_user(
        username=username,
        password=password,
        active=False,
        extra={'bar': 'baz'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is False
    assert new_user['extra'] == {'bar': 'baz'}
    assert sys_db.user(username) == new_user

    # Update a missing user
    with pytest.raises(UserUpdateError) as err:
        sys_db.update_user(
            username=generate_username(),
            password=generate_string()
        )
    assert err.value.http_code == 404

    # Replace an existing user
    new_user = sys_db.replace_user(
        username=username,
        password=password,
        active=False,
        extra={'baz': 'qux'},
    )
    assert new_user['username'] == username
    assert new_user['active'] is False
    assert new_user['extra'] == {'baz': 'qux'}
    assert sys_db.user(username) == new_user

    # Replace a missing user
    with pytest.raises(UserReplaceError) as err:
        sys_db.replace_user(
            username=generate_username(),
            password=generate_string()
        )
    assert err.value.http_code == 404

    # Delete an existing user
    assert sys_db.delete_user(username) is True

    # Delete a missing user
    with pytest.raises(UserDeleteError) as err:
        sys_db.delete_user(username, ignore_missing=False)
    assert err.value.http_code == 404
    assert sys_db.delete_user(username, ignore_missing=True) is False


def test_user_change_password(client, sys_db):
    username = generate_username()
    password1 = generate_string()
    password2 = generate_string()

    sys_db.create_user(username, password1)
    sys_db.update_permission(username, 'rw', sys_db.name)

    db1 = client.db(sys_db.name, username, password1, verify=False)
    db2 = client.db(sys_db.name, username, password2, verify=False)

    # Check authentication
    db1.properties()
    with pytest.raises(DatabasePropertiesError) as err:
        db2.properties()
    assert err.value.http_code in {401, 403}

    # Update the user password and check again
    sys_db.update_user(username, password2)
    db2.properties()
    with pytest.raises(DatabasePropertiesError) as err:
        db1.properties()
    assert err.value.http_code in {401, 403}

    # Replace the user password and check again
    sys_db.update_user(username, password1)
    db1.properties()
    with pytest.raises(DatabasePropertiesError) as err:
        db2.properties()
    assert err.value.http_code in {401, 403}


def test_user_create_with_new_database(client, sys_db):
    db_name = generate_database_name()

    username1 = generate_username()
    username2 = generate_username()
    username3 = generate_username()

    password1 = generate_string()
    password2 = generate_string()
    password3 = generate_string()

    result = sys_db.create_database(
        name=db_name,
        users=[
            {'username': username1, 'password': password1, 'active': True},
            {'username': username2, 'password': password2, 'active': True},
            {'username': username3, 'password': password3, 'active': False},
        ]
    )
    assert result is True

    # Test if the users were created properly
    usernames = extract('username', sys_db.users())
    assert all(u in usernames for u in [username1, username2, username3])

    # Test if the first user has access to the database
    db = client.db(db_name, username1, password1, verify=False)
    db.properties()

    # Test if the second user also has access to the database
    db = client.db(db_name, username2, password2, verify=False)
    db.properties()

    # Test if the third user has access to the database (should not)
    db = client.db(db_name, username3, password3, verify=False)
    with pytest.raises(DatabasePropertiesError) as err:
        db.properties()
    assert err.value.http_code in {401, 403}
