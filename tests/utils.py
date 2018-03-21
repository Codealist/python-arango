from __future__ import absolute_import, unicode_literals

from uuid import uuid4

from arango.cursor import Cursor


def generate_database_name():
    """Generate and return a random database name.

    :return: Random database name.
    :rtype: str or unicode
    """
    return 'test_database_{}'.format(uuid4().hex)


def generate_collection_name():
    """Generate and return a random collection name.

    :return: Random collection name.
    :rtype: str or unicode
    """
    return 'test_collection_{}'.format(uuid4().hex)


def generate_graph_name():
    """Generate and return a random graph name.

    :return: Random graph name.
    :rtype: str or unicode
    """
    return 'test_graph_{}'.format(uuid4().hex)


def generate_document_key():
    """Generate and return a random document key.

    :return: Random document key.
    :rtype: str or unicode
    """
    return 'test_document_{}'.format(uuid4().hex)


def generate_task_name():
    """Generate and return a random task name.

    :return: Random task name.
    :rtype: str or unicode
    """
    return 'test_task_{}'.format(uuid4().hex)


def generate_task_id():
    """Generate and return a random task ID.

    :return: Random task ID
    :rtype: str or unicode
    """
    return 'test_task_id_{}'.format(uuid4().hex)


def generate_username():
    """Generate and return a random username.

    :return: Random username.
    :rtype: str or unicode
    """
    return 'test_user_{}'.format(uuid4().hex)


def generate_string():
    """Generate and return a random unique string.

    :return: Random unique string.
    :rtype: str or unicode
    """
    return uuid4().hex


def purge_test_databases(sys_db):
    """Remove all databases used for testing.

    :param sys_db: System database.
    :type sys_db: arango.database.Database
    """
    for database in sys_db.databases():
        if database.startswith('test_database'):
            sys_db.delete_database(database)


def purge_test_collections(sys_db):
    """Remove all databases used for testing.

    :param sys_db: System database.
    :type sys_db: arango.database.Database
    """
    for collection in sys_db.collections():
        if collection['name'].startswith('test_collection'):
            sys_db.delete_collection(collection)


def purge_test_tasks(sys_db):
    """Remove all server tasks used for testing.

    :param sys_db: System database.
    :type sys_db: arango.database.Database
    """
    for task in sys_db.tasks():
        if task['name'].startswith('test_task'):
            sys_db.delete_task(task['id'], ignore_missing=True)


def clean_test_users(sys_db):
    """Remove all users used for testing.

    :param sys_db: System database.
    :type sys_db: arango.database.Database
    """
    for user in sys_db.users():
        if user['username'].startswith('test_user'):
            sys_db.delete_user(user['username'], ignore_missing=True)


def clean(obj):
    """Return the document(s) with all extra system keys stripped.

    :param obj: document(s)
    :type obj: list or dict or object
    :return: Document(s) with the system keys stripped
    :rtype: list or dict or object
    """
    if isinstance(obj, (Cursor, list)):
        docs = [clean(d) for d in obj]
        return sorted(docs, key=lambda doc: doc['_key'])

    if isinstance(obj, dict):
        return {
            field: value for field, value in obj.items()
            if field in {'_key', '_from', '_to'} or not field.startswith('_')
        }
    return obj


def extract(key, items):
    """Return the sorted values from dicts using the given key.

    :param key: Dictionary key
    :type key: str or unicode
    :param items: Items to filter.
    :type items: [dict]
    :return: Set of values.
    :rtype: [str or unicode]
    """
    return sorted(item[key] for item in items)
