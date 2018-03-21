from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.exceptions import (
    TaskCreateError,
    TaskDeleteError,
    TaskGetError,
    TaskListError
)
from tests.utils import (
    generate_task_name,
    generate_task_id
)

test_command = 'require("@arangodb").print(params);'


def test_task_list(db, bad_db):
    for task in db.tasks():
        assert task['database'] in db.databases()
        assert task['type'] in {'periodic', 'timed'}
        assert isinstance(task['id'], string_types)
        assert isinstance(task['name'], string_types)
        assert isinstance(task['created'], float)
        assert isinstance(task['command'], string_types)

    with pytest.raises(TaskListError):
        bad_db.tasks()


def test_task_get(db):
    # Test get existing tasks
    for task in db.tasks():
        assert db.task(task['id']) == task

    # Test get missing task
    with pytest.raises(TaskGetError) as err:
        db.task(generate_task_id())
    assert err.value.http_code == 404


def test_task_create(db):
    # Test create task with random ID
    task_name = generate_task_name()
    new_task = db.create_task(
        name=task_name,
        command=test_command,
        params={'foo': 1, 'bar': 2},
        offset=1,
    )
    assert new_task['name'] == task_name
    assert 'print(params)' in new_task['command']
    assert new_task['type'] == 'timed'
    assert new_task['database'] == db.name
    assert isinstance(new_task['created'], float)
    assert isinstance(new_task['id'], string_types)
    assert db.task(new_task['id']) == new_task

    # Test create task with specific ID
    task_name = generate_task_name()
    task_id = generate_task_id()
    new_task = db.create_task(
        name=task_name,
        command=test_command,
        params={'foo': 1, 'bar': 2},
        offset=1,
        period=10,
        task_id=task_id
    )
    assert new_task['name'] == task_name
    assert new_task['id'] == task_id
    assert 'print(params)' in new_task['command']
    assert new_task['type'] == 'periodic'
    assert new_task['database'] == db.name
    assert isinstance(new_task['created'], float)
    assert db.task(new_task['id']) == new_task

    # Test create duplicate task
    with pytest.raises(TaskCreateError) as err:
        db.create_task(
            name=task_name,
            command=test_command,
            params={'foo': 1, 'bar': 2},
            task_id=task_id
        )
    assert err.value.http_code == 409


def test_task_delete(db):
    # Set up a test task to delete
    task_name = generate_task_name()
    task_id = generate_task_id()
    db.create_task(
        name=task_name,
        command=test_command,
        params={'foo': 1, 'bar': 2},
        task_id=task_id,
        period=10
    )

    # Test delete existing task
    assert db.delete_task(task_id) is True
    with pytest.raises(TaskGetError) as err:
        db.task(task_id)
    assert err.value.http_code == 404

    # Test delete missing task without ignore_missing
    with pytest.raises(TaskDeleteError) as err:
        db.delete_task(task_id, ignore_missing=False)
    assert err.value.http_code == 404

    # Test delete missing task with ignore_missing
    assert db.delete_task(task_id, ignore_missing=True) is False
