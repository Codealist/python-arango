from __future__ import absolute_import, unicode_literals

from datetime import datetime

from arango.api import APIWrapper
from arango.aql import AQL
from arango.async import AsyncExecutor
from arango.batch import Batch
from arango.collection import Collection
from arango.exceptions import (
    AsyncJobClearError,
    AsyncJobListError,
    CollectionCreateError,
    CollectionDeleteError,
    CollectionListError,
    DatabaseDeleteError,
    DatabaseCreateError,
    DatabaseListError,
    DatabasePropertiesError,
    GraphListError,
    GraphCreateError,
    GraphDeleteError,
    PregelJobCreateError,
    PregelJobDeleteError,
    PregelJobGetError,
    ServerConnectionError,
    ServerEndpointsError,
    ServerDetailsError,
    ServerEchoError,
    ServerLogLevelError,
    ServerLogLevelSetError,
    ServerReadLogError,
    ServerReloadRoutingError,
    ServerTargetVersionError,
    ServerRoleError,
    ServerRunTestsError,
    ServerShutdownError,
    ServerStatisticsError,
    ServerTimeError,
    ServerVersionError,
    TaskCreateError,
    TaskDeleteError,
    TaskGetError,
    TaskListError,
    TransactionExecuteError,
    PermissionGetError,
    UserCreateError,
    UserDeleteError,
    UserGetError,
    PermissionUpdateError,
    UserListError,
    PermissionDeleteError,
    UserReplaceError,
    UserUpdateError,
    ServerEngineError)
from arango.foxx import Foxx
from arango.graph import Graph
from arango.request import Request
from arango.transaction import Transaction
from arango.utils import is_dict
from arango.wal import WAL


class Database(APIWrapper):
    """ArangoDB database.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.api.APIExecutor
    """

    def __init__(self, connection, executor):
        super(Database, self).__init__(connection, executor)
        self._aql = AQL(connection, executor)
        self._wal = WAL(connection, executor)
        self._foxx = Foxx(connection, executor)

    def __repr__(self):
        return '<Database {}>'.format(self.name)

    def __getitem__(self, name):
        """Return the collection wrapper.

        :param name: Collection name.
        :type name: str or unicode
        :return: Collection wrapper.
        :rtype: arango.collection.Collection
        """
        return self.collection(name)

    @property
    def name(self):
        """Return the database name.

        :return: Database name.
        :rtype: str or unicode
        """
        return self._conn.database

    @property
    def username(self):
        """Return the username used for authentication.

        :return: Username used for authentication.
        :rtype: str or unicode
        """
        return self._conn.username

    @property
    def aql(self):
        """Return the AQL wrapper used to execute AQL statements.

        See :class:`arango.aql.AQL` for more information.

        :return: AQL wrapper.
        :rtype: arango.aql.AQL
        """
        return self._aql

    @property
    def wal(self):
        """Return the WAL (write-ahead log) wrapper.

        See :class:`arango.wal.WAL` for more information.

        :return: WAL wrapper.
        :rtype: arango.wal.WAL
        """
        return self._wal

    @property
    def foxx(self):
        """Return the Foxx wrapper.

        See :class:`arango.foxx.Foxx` for more information.

        :return: Foxx wrapper.
        :rtype:`arango.foxx.Foxx
        """
        return self._foxx

    def properties(self):
        """Return the database properties.

        :return: Database properties.
        :rtype: dict
        :raise arango.exceptions.DatabasePropertiesError: If retrieval
            fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/database/current',
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DatabasePropertiesError(resp)
            result = resp.body['result']
            result['system'] = result.pop('isSystem')
            return result

        return self._execute(request, response_handler)

    def begin_async(self, return_result=True):
        """Begin async API execution.

        :param return_result: If set to True, results of API calls are stored
            server-side and instances of :class:`arango.async.AsyncJob` are
            returned to the user for progress tracking and result retrieval.
            If set to False, no results are stored server-side and no jobs are
            returned to the user.
        :type return_result: bool
        :return: New database wrapper. API calls made using this database are
            queued up server-side and executed asynchronously.
        :rtype: arango.database.Database
        """
        return Database(self._conn, AsyncExecutor(return_result))

    def begin_batch(self, return_result=True):
        """Begin batch API execution.

        :param return_result: If set to True, results of API calls are stored
            client-side and instances of :class:`arango.batch.BatchJob` are
            returned to the user for progress tracking and result retrieval.
            The job instances are populated with results on commit. If set to
            False, results are ignored and no jobs are returned to the user.
            This saves memory when results are not required.
        :type return_result: bool
        :return: New database wrapper. API calls made using this database are
            queued up client-side and executed in one go in when committed.
        :rtype: arango.database.Database
        """
        return Batch(self._conn, return_result)

    def begin_transaction(self, timeout=None, sync=None, return_result=True):
        """Begin transaction.

        :param timeout: Timeout on collection locks.
        :type timeout: int
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param return_result: If set to True, API calls are queued client-side
            and :class:`arango.transaction.TransactionJob` instances are
            returned to user. Job instances are populated with the results on
            commit. If set to False, requests are queued and executed, but
            results are not saved and job objects are not returned to the user.
        :type return_result: bool
        """
        return Transaction(
            connection=self._conn,
            timeout=timeout,
            sync=sync,
            return_result=return_result
        )

    def execute_transaction(self,
                            command,
                            params=None,
                            read=None,
                            write=None,
                            sync=None,
                            timeout=None,
                            max_size=None,
                            allow_implicit=None,
                            autocommit_ops=None,
                            autocommit_size=None):
        """Execute a raw Javascript code in a transaction.

        :param command: Javascript code to execute.
        :type command: str or unicode
        :param read: Names of collections read during transaction.
            If **allow_implicit** is set to True, any undeclared collections
            are loaded lazily.
        :type read: [str or unicode]
        :param write: Names of collections where data is written
            during transaction. Transaction fails on undeclared collections.
        :type write: [str or unicode]
        :param params: Optional arguments passed to **action**.
        :type params: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param timeout: Timeout for waiting on collection locks. If set
            to 0, the ArangoDB server waits indefinitely. If not set, system
            default value is used.
        :type timeout: int
        :param max_size: Maximum transaction size limit in bytes. Applies only
            to RocksDB storage engine.
        :type max_size: int
        :param allow_implicit: If set to True, undeclared read collections are
            loaded lazily. If set to False, transaction fails on undeclared
            collections.
        :type allow_implicit: bool
        :param autocommit_ops: Maximum number of operations after which an
            intermediate commit is performed automatically. Applies only to
            RocksDB storage engine.
        :type autocommit_ops: int
        :param autocommit_size: Maximum total size of operations after which an
            intermediate commit is performed automatically. Applies only to
            RocksDB storage engine.
        :type autocommit_size: int
        :return: Return value of the code defined in **action**.
        :rtype: str or unicode
        :raise arango.exceptions.TransactionExecuteError: If execution fails.
        """
        collections = {'allowImplicit': allow_implicit}
        if read is not None:
            collections['read'] = read
        if write is not None:
            collections['write'] = write

        data = {'action': command}
        if collections:
            data['collections'] = collections
        if params is not None:
            data['params'] = params
        if timeout is not None:
            data['lockTimeout'] = timeout
        if sync is not None:
            data['waitForSync'] = sync
        if max_size is not None:
            data['maxTransactionSize'] = max_size
        if autocommit_ops is not None:
            data['intermediateCommitCount'] = autocommit_ops
        if autocommit_size is not None:
            data['intermediateCommitSize'] = autocommit_size

        request = Request(
            method='post',
            endpoint='/_api/transaction',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TransactionExecuteError(resp)
            return resp.body.get('result')

        return self._execute(request, response_handler)

    def version(self):
        """Return ArangoDB server version.

        :return: Server version.
        :rtype: str or unicode
        :raise arango.exceptions.ServerVersionError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/version',
            params={'details': False},
            command='db._version(false)'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerVersionError(resp)
            if is_dict(resp.body):
                return resp.body['version']
            return resp.body

        return self._execute(request, response_handler)

    def details(self):
        """Return ArangoDB server details.

        :return: Server details
        :rtype: dict
        :raise arango.exceptions.ServerDetailsError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/version',
            params={'details': True},
            command='db._version(true)'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerDetailsError(resp)
            return resp.body.get('details', resp.body)

        return self._execute(request, response_handler)

    def target_version(self):
        """Return required version of target database.

        :return: Required version of target database.
        :rtype: str or unicode
        :raise arango.exceptions.ServerTargetVersionError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_admin/database/target-version'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerTargetVersionError(resp)
            return resp.body['version']

        return self._execute(request, response_handler)

    def endpoints(self):
        """Return the information about all coordinate endpoints.

        :return: List of endpoints.
        :rtype: [str or unicode]
        :raise arango.exceptions.ServerEndpointsError: If retrieval fails.

        .. note::
            This is for cluster mode only.
        """
        request = Request(
            method='get',
            endpoint='/_api/cluster/endpoints'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerEndpointsError(resp)
            return [item['endpoint'] for item in resp.body['endpoints']]

        return self._execute(request, response_handler)

    def engine(self):
        """Return the database engine information.

        :return: Database engine information.
        :rtype: str or unicode
        :raise arango.exceptions.ServerEngineError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/engine',
            command='db._engine()'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerEngineError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def ping(self):
        """Ping the ArangoDB server.

        :return: Response code from server.
        :rtype: int
        :raise arango.exceptions.ServerConnectionError: If ping fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection',
        )

        def response_handler(resp):
            code = resp.status_code
            if code in {401, 403}:
                raise ServerConnectionError(
                    message='bad username and/or password')
            if not resp.is_success:
                message = resp.error_message or 'bad server response'
                raise ServerConnectionError(message=message)
            return code

        return self._execute(request, response_handler)

    def statistics(self, description=False):
        """Return the server statistics.

        :return: Server statistics.
        :rtype: dict
        :raise arango.exceptions.ServerStatisticsError: If retrieval fails.
        """
        if description:
            url = '/_admin/statistics-description'
        else:
            url = '/_admin/statistics'

        request = Request(
            method='get',
            endpoint=url
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerStatisticsError(resp)
            resp.body.pop('code')
            resp.body.pop('error')
            return resp.body

        return self._execute(request, response_handler)

    def role(self):
        """Return the role of the server in the cluster if any.

        :return: Server role which can be "SINGLE" (server not in a cluster),
            "COORDINATOR" (cluster coordinator), "PRIMARY", "SECONDARY" or
            "UNDEFINED".
        :rtype: str or unicode
        :raise arango.exceptions.ServerRoleError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_admin/server/role'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerRoleError(resp)
            return resp.body.get('role')

        return self._execute(request, response_handler)

    def time(self):
        """Return the current server system time.

        :return: Server system time
        :rtype: datetime.datetime
        :raise arango.exceptions.ServerTimeError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_admin/time'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerTimeError(resp)
            return datetime.fromtimestamp(resp.body['time'])

        return self._execute(request, response_handler)

    def echo(self):
        """Return information on the last request (headers, payload etc.)

        :return: Details of the last request
        :rtype: dict
        :raise arango.exceptions.ServerEchoError: If last request cannot
            be retrieved from the server
        """
        request = Request(
            method='get',
            endpoint='/_admin/echo'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerEchoError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def shutdown(self):  # pragma: no cover
        """Initiate the server shutdown sequence.

        :return: whether the server was shutdown successfully
        :rtype: bool
        :raise arango.exceptions.ServerShutdownError: If shutdown fails.
        """
        request = Request(
            method='delete',
            endpoint='/_admin/shutdown'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerShutdownError(resp)
            return True

        return self._execute(request, response_handler)

    def run_tests(self, tests):  # pragma: no cover
        """Run the available unittests on the server.

        :param tests: List of files containing the test suites.
        :type tests: [str or unicode]
        :return: Test results.
        :rtype: dict
        :raise arango.exceptions.ServerRunTestsError: If execution fails.
        """
        request = Request(
            method='post',
            endpoint='/_admin/test',
            data={'tests': tests}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerRunTestsError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def read_log(self,
                 upto=None,
                 level=None,
                 start=None,
                 size=None,
                 offset=None,
                 search=None,
                 sort=None):
        """Read the global log from the server.

        :param upto: return the log entries up to the given level (mutually
            exclusive with argument **level**), which must be "fatal",
            "error", "warning", "info" (default) or "debug"
        :type upto: str or unicode or int
        :param level: return the log entries of only the given level (mutually
            exclusive with **upto**), which must be "fatal", "error",
            "warning", "info" (default) or "debug"
        :type level: str or unicode or int
        :param start: return the log entries whose ID is greater or equal to
            the given value
        :type start: int
        :param size: restrict the size of the result to the given value (this
            setting can be used for pagination)
        :type size: int
        :param offset: Number of entries to skip initially (this setting
            can be setting can be used for pagination)
        :type offset: int
        :param search: return only the log entries containing the given text
        :type search: str or unicode
        :param sort: sort the log entries according to the given fashion, which
            can be "sort" or "desc"
        :type sort: str or unicode
        :return: Server log entries
        :rtype: dict
        :raise arango.exceptions.ServerReadLogError: If server log entries
            cannot be read
        """
        params = dict()
        if upto is not None:
            params['upto'] = upto
        if level is not None:
            params['level'] = level
        if start is not None:
            params['start'] = start
        if size is not None:
            params['size'] = size
        if offset is not None:
            params['offset'] = offset
        if search is not None:
            params['search'] = search
        if sort is not None:
            params['sort'] = sort

        request = Request(
            method='get',
            endpoint='/_admin/log',
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerReadLogError(resp)
            if 'totalAmount' in resp.body:
                resp.body['total_amount'] = resp.body.pop('totalAmount')
            return resp.body

        return self._execute(request, response_handler)

    def log_levels(self):
        """Return the current logging levels.

        :return: Current logging levels.
        :rtype: dict
        """
        request = Request(
            method='get',
            endpoint='/_admin/log/level'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerLogLevelError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def set_log_levels(self, **kwargs):
        """Set the logging levels.

        This method takes arbitrary keyword arguments where the keys are the
        logger names and the values are the logging levels. For example:

        .. code-block:: python

            arango.set_log_level(
                agency='DEBUG',
                collector='INFO',
                threads='WARNING'
            )

        :return: New logging levels
        :rtype: dict

        .. note::
            Keys that are not valid logger names are ignored.
        """
        request = Request(
            method='put',
            endpoint='/_admin/log/level',
            data=kwargs
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerLogLevelSetError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def reload_routing(self):
        """Reload the routing information from the collection *routing*.

        :return: whether the routing was reloaded successfully
        :rtype: bool
        :raise arango.exceptions.ServerReloadRoutingError: If routing
            cannot be reloaded
        """

        request = Request(
            method='post',
            endpoint='/_admin/routing/reload'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerReloadRoutingError(resp)
            return 'error' not in resp.body

        return self._execute(request, response_handler)

    #######################
    # Database Management #
    #######################

    def databases(self):
        """Return the database names.

        :return: Database names.
        :rtype: list
        :raise arango.exceptions.DatabaseListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/database',
            command='db._databases()'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DatabaseListError(resp)
            if is_dict(resp.body):
                return resp.body['result']
            return resp.body

        return self._execute(request, response_handler)

    def create_database(self, name, users=None):
        """Create a new database.

        :param name: Database name.
        :type name: str or unicode
        :param users: List of users with access to the new database, where
            each user is represented by a dictionary with fields "username",
            "password", "active" and "extra". If not set, only the admin and
            current user are granted access by default. Here is an example
            entry for parameter **users**:

            .. code-block:: python

                {
                    'username': 'john',
                    'password': 'password',
                    'active': True,
                    'extra': {'Department': 'IT'}
                }

        :type users: [dict]
        :return: Database wrapper.
        :rtype: arango.database.Database
        :raise arango.exceptions.DatabaseCreateError: If create fails.
        """
        data = {'name': name}
        if users is not None:
            data['users'] = [{
                'username': user['username'],
                'passwd': user['password'],
                'active': user.get('active', True),
                'extra': user.get('extra', {})
            } for user in users]

        request = Request(
            method='post',
            endpoint='/_api/database',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DatabaseCreateError(resp)
            return resp.body['result']

        return self._execute(request, response_handler)

    def delete_database(self, name, ignore_missing=False):
        """Delete the database of the specified name.

        :param name: Database name.
        :type name: str or unicode
        :param ignore_missing: Do not raise exception on missing database.
        :type ignore_missing: bool
        :return: True if the database was deleted successfully.
        :rtype: bool
        :raise arango.exceptions.DatabaseDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/database/{}'.format(name)
        )

        def response_handler(resp):
            if not resp.is_success:
                if resp.status_code == 404 and ignore_missing:
                    return False
                raise DatabaseDeleteError(resp)
            return resp.body['result']

        return self._execute(request, response_handler)

    #########################
    # Collection Management #
    #########################

    def collection(self, name):
        """Return the collection wrapper.

        :param name: Collection name.
        :type name: str or unicode
        :return: Collection wrapper.
        :rtype: arango.collection.Collection
        """
        return Collection(self._conn, self._executor, name)

    def collections(self):
        """Return the collections in the database.

        :return: Details of the collections in the database.
        :rtype: [dict] or [str or unicode]
        :raise arango.exceptions.CollectionListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/collection'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionListError(resp)
            return [{
                'id': col['id'],
                'name': col['name'],
                'system': col['isSystem'],
                'type': Collection.types[col['type']],
                'status': Collection.statuses[col['status']],
            } for col in map(dict, resp.body['result'])]

        return self._execute(request, response_handler)

    def create_collection(self,
                          name,
                          sync=False,
                          compact=True,
                          system=False,
                          journal_size=None,
                          edge=False,
                          volatile=False,
                          user_keys=True,
                          key_increment=None,
                          key_offset=None,
                          key_generator='traditional',
                          shard_fields=None,
                          shard_count=None,
                          index_bucket_count=None,
                          replication_factor=None):
        """Create a new collection.

        :param name: Collection name.
        :type name: str or unicode
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param compact: Whether the collection is compacted.
        :type compact: bool
        :param system: Whether the collection is a system collection. Starting
            from ArangoDB version 3.1+, system collections must have a name
            with a leading underscore "_" character.
        :type system: bool
        :param journal_size: Maximum size of the journal in bytes.
        :type journal_size: int
        :param edge: Whether the collection is an edge collection.
        :type edge: bool
        :param volatile: Whether The collection is in-memory only.
        :type volatile: bool
        :param key_generator: Used for generating document keys. Allowed values
            are "traditional" or "autoincrement".
        :type key_generator: str or unicode
        :param user_keys: Whether to allow users to supply the document keys.
        :type user_keys: bool
        :param key_increment: Key increment value. Applies only when the value
            of **key_generator** is set to "autoincrement".
        :type key_increment: int
        :param key_offset: Key offset value. Applies only when the value of
            **key_generator** is set to "autoincrement".
        :type key_offset: int
        :param shard_fields: Field(s) used to determine the target shard.
        :type shard_fields: [str or unicode]
        :param shard_count: Number of shards to create.
        :type shard_count: int
        :param index_bucket_count: Number of buckets into which indexes
            using hash tabled are split. Yhe default is 16, and this number
            has to be a power of 2 and less than or equal to 1024. Gor very
            large collections, one should increase this to avoid long pauses
            when the hash table has to be initially built or re-sized, since
            buckets are re-sized individually and can be initially built in
            parallel. To instance, 64 may be a sensible value for a collection
            with 100,000,000 documents.
        :type index_bucket_count: int
        :param replication_factor: Number of copies of each shard on
            different servers in a cluster. Allowed values are:

            .. code-block:: none

                1: Only one copy is kept (no synchronous replication). This
                   is the default value.

                k: k-1 replicas are kept and any two copies are replicated
                   across different DBServers synchronously, meaning every
                   write to the master is copied to all slaves before the
                   operation is reported successful.

        :type replication_factor: int
        :return: Collection wrapper.
        :rtype: arango.collection.Collection
        :raise arango.exceptions.CollectionCreateError: If create fails.
        """
        key_options = {'type': key_generator, 'allowUserKeys': user_keys}
        if key_increment is not None:
            key_options['increment'] = key_increment
        if key_offset is not None:
            key_options['offset'] = key_offset

        data = {
            'name': name,
            'waitForSync': sync,
            'doCompact': compact,
            'isSystem': system,
            'isVolatile': volatile,
            'keyOptions': key_options
        }

        if edge:
            data['type'] = 3
        else:
            data['type'] = 2

        if journal_size is not None:
            data['journalSize'] = journal_size
        if shard_count is not None:
            data['numberOfShards'] = shard_count
        if shard_fields is not None:
            data['shardKeys'] = shard_fields
        if index_bucket_count is not None:
            data['indexBuckets'] = index_bucket_count
        if replication_factor is not None:
            data['replicationFactor'] = replication_factor

        request = Request(
            method='post',
            endpoint='/_api/collection',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise CollectionCreateError(resp)
            return self.collection(name)

        return self._execute(request, response_handler)

    def delete_collection(self, name, ignore_missing=False, system=None):
        """Delete a collection.

        :param name: Collection name.
        :type name: str or unicode
        :param ignore_missing: Do not raise an exception on missing collection.
        :type ignore_missing: bool
        :param system: Whether the collection is a system collection. Only
            only available with ArangoDB 3.1+.
        :type system: bool
        :return: True if the deletion was successful.
        :rtype: bool
        :raise arango.exceptions.CollectionDeleteError: If delete fails.
        """
        params = {}
        if system is not None:
            params['isSystem'] = system

        request = Request(
            method='delete',
            endpoint='/_api/collection/{}'.format(name),
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                if not (resp.status_code == 404 and ignore_missing):
                    raise CollectionDeleteError(resp)
            return not resp.body['error']

        return self._execute(request, response_handler)

    ####################
    # Graph Management #
    ####################

    def graph(self, name):
        """Return the graph wrapper.

        :param name: Graph name.
        :type name: str or unicode
        :return: Graph wrapper.
        :rtype: arango.graph.Graph
        """
        return Graph(self._conn, self._executor, name)

    def graphs(self):
        """List all graphs in the database.

        :return: Graphs in the database.
        :rtype: dict or [str or unicode]
        :raise arango.exceptions.GraphListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GraphListError(resp)
            return [
                {
                    'name': record['_key'],
                    'revision': record['_rev'],
                    'edge_definitions': record['edgeDefinitions'],
                    'orphan_collections': record['orphanCollections'],
                    'smart': record.get('isSmart'),
                    'smart_field': record.get('smartGraphAttribute'),
                    'shard_count': record.get('numberOfShards')
                } for record in map(dict, resp.body['graphs'])
            ]

        return self._execute(request, response_handler)

    def create_graph(self,
                     name,
                     edge_definitions=None,
                     orphan_collections=None,
                     smart=None,
                     smart_field=None,
                     shard_count=None):
        """Create a new graph in the database.

        :param name: Graph name.
        :type name: str or unicode
        :param edge_definitions: List of edge definitions. An edge definition
            should look like this:

            .. code-block:: python

                {
                    'name': 'edge_collection_name',
                    'from_collections': ['from_vertex_collection_name'],
                    'to_collections': ['to_vertex_collection_name']
                }

        :type edge_definitions: [dict]
        :param orphan_collections: Names of additional vertex collections.
        :type orphan_collections: [str or unicode]
        :param smart: If set to True, sharding is enabled (see parameter
            **smart_field** below). This is only for the enterprise version of
            ArangoDB.
        :type smart: bool
        :param smart_field: Document field used to shard the vertices of
            the graph. To use this, parameter **smart** must be set to True and
            every vertex in the graph must have the smart field. This is only
            for the enterprise version of ArangoDB.
        :type smart_field: str or unicode
        :param shard_count: Number of shards used for every collection in
            the graph. To use this, parameter **smart** must be set to True and
            every vertex in the graph must have the smart field. The number
            cannot be modified later once set. This is only for the enterprise
            version of ArangoDB.
        :type shard_count: int
        :return: Graph wrapper.
        :rtype: arango.graph.Graph
        :raise arango.exceptions.GraphCreateError: If create fails.
        """
        data = {'name': name}
        if edge_definitions is not None:
            data['edgeDefinitions'] = [{
                'collection': definition['name'],
                'from': definition['from_collections'],
                'to': definition['to_collections']
            } for definition in edge_definitions]
        if orphan_collections is not None:
            data['orphanCollections'] = orphan_collections
        if smart is not None:  # pragma: no cover
            data['isSmart'] = smart
        if smart_field is not None:  # pragma: no cover
            data['smartGraphAttribute'] = smart_field
        if shard_count is not None:  # pragma: no cover
            data['numberOfShards'] = shard_count

        request = Request(
            method='post',
            endpoint='/_api/gharial',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise GraphCreateError(resp)
            return Graph(self._conn, self._executor, name)

        return self._execute(request, response_handler)

    def delete_graph(self, name, ignore_missing=False, drop_collections=None):
        """Drop the graph of the given name from the database.

        :param name: Name of the graph to delete/drop.
        :type name: str or unicode
        :param ignore_missing: Ignore HTTP 404 (graph not found) from the
            server. If this is set to True an exception is not raised.
        :type ignore_missing: bool
        :param drop_collections: Whether to drop the collections of the graph
            as well. The collections can only be dropped if they are not in use
            by other graphs.
        :type drop_collections: bool
        :return: Whether the deletion was successful.
        :rtype: bool
        :raise arango.exceptions.GraphDeleteError: If graph cannot be
            deleted from the database
        """
        params = {}
        if drop_collections is not None:
            params['dropCollections'] = drop_collections

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}'.format(name),
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                if not (resp.status_code == 404 and ignore_missing):
                    raise GraphDeleteError(resp)
            return not resp.body['error']

        return self._execute(request, response_handler)

    ###################
    # Task Management #
    ###################

    def tasks(self):
        """Return all server tasks that are currently active.

        :return: Server tasks that are currently active.
        :rtype: [dict]
        :raise arango.exceptions.TaskListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/tasks'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TaskListError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def task(self, task_id):
        """Return the active server task with the given id.

        :param task_id: ID of the server task
        :type task_id: str or unicode
        :return: Details on the active server task
        :rtype: dict
        :raise arango.exceptions.TaskGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/tasks/{}'.format(task_id)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TaskGetError(resp)
            resp.body.pop('code', None)
            resp.body.pop('error', None)
            return resp.body

        return self._execute(request, response_handler)

    # TODO verify which arguments are optional
    def create_task(self,
                    name,
                    command,
                    params=None,
                    period=None,
                    offset=None,
                    task_id=None):
        """Create a new server task.

        :param name: Name of the server task.
        :type name: str or unicode
        :param command: Javascript code to execute.
        :type command: str or unicode
        :param params: Parameters passed into the command.
        :type params: dict
        :param period: Number of seconds to wait between executions. If set
            to 0, the new task will be "timed", meaning it will execute only
            once and be deleted automatically afterwards.
        :type period: int
        :param offset: Initial delay before execution in seconds.
        :type offset: int
        :param task_id: Pre-defined ID for the new server task.
        :type task_id: str or unicode
        :return: Details on the new task.
        :rtype: dict
        :raise arango.exceptions.TaskCreateError: If create fails.
        """
        data = {
            'name': name,
            'command': command
        }
        if params is not None:
            data['params'] = params
        if task_id is not None:
            data['id'] = task_id
        if period is not None:
            data['period'] = period
        if offset is not None:
            data['offset'] = offset

        if task_id is None:
            task_id = ''

        request = Request(
            method='post',
            endpoint='/_api/tasks/{}'.format(task_id),
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise TaskCreateError(resp)
            resp.body.pop('code', None)
            resp.body.pop('error', None)
            return resp.body

        return self._execute(request, response_handler)

    def delete_task(self, task_id, ignore_missing=False):
        """Delete the server task specified by ID.

        :param task_id: ID of the server task.
        :type task_id: str or unicode
        :param ignore_missing: Do not raise an exception on missing task.
        :type ignore_missing: bool
        :return: True if the task was successfully deleted.
        :rtype: bool
        :raise arango.exceptions.TaskDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/tasks/{}'.format(task_id)
        )

        def response_handler(resp):
            if not resp.is_success:
                if not (resp.status_code == 404 and ignore_missing):
                    raise TaskDeleteError(resp)
            return not resp.body['error']

        return self._execute(request, response_handler)

    ###################
    # User Management #
    ###################

    def users(self):
        """Return the details of all users.

        :return: Details of all users or just the usernames.
        :rtype: [dict] or [str or unicode]
        :raise arango.exceptions.UserListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/user'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserListError(resp)
            return [{
                'username': record['user'],
                'active': record['active'],
                'extra': record['extra'],
            } for record in resp.body['result']]

        return self._execute(request, response_handler)

    def user(self, username):
        """Return the details of a user.

        :param username: Details of the user
        :type username: str or unicode
        :return: User details
        :rtype: dict
        :raise arango.exceptions.UserGetError: If retrieval fails
        """
        request = Request(
            method='get',
            endpoint='/_api/user/{}'.format(username)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserGetError(resp)
            return {
                'username': resp.body['user'],
                'active': resp.body['active'],
                'extra': resp.body['extra']
            }

        return self._execute(request, response_handler)

    def create_user(self, username, password, active=True, extra=None):
        """Create a new user.

        :param username: New username.
        :type username: str or unicode
        :param password: Password.
        :type password: str or unicode
        :param active: Whether the user is active.
        :type active: bool
        :param extra: Any extra data on the user.
        :type extra: dict
        :return: Details of the new user.
        :rtype: dict
        :raise arango.exceptions.UserCreateError: If create fails.
        """
        data = {'user': username, 'passwd': password, 'active': active}
        if extra is not None:
            data['extra'] = extra

        request = Request(
            method='post',
            endpoint='/_api/user',
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserCreateError(resp)
            return {
                'username': resp.body['user'],
                'active': resp.body['active'],
                'extra': resp.body['extra'],
            }

        return self._execute(request, response_handler)

    def update_user(self, username, password=None, active=None, extra=None):
        """Update an existing user.

        :param username: Username.
        :type username: str or unicode
        :param password: New password.
        :type password: str or unicode
        :param active: Whether the user is active.
        :type active: bool
        :param extra: Any extra data on the user.
        :type extra: dict
        :return: Details of the updated user.
        :rtype: dict
        :raise arango.exceptions.UserUpdateError: If update fails.
        """
        data = {}
        if password is not None:
            data['passwd'] = password
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra

        request = Request(
            method='patch',
            endpoint='/_api/user/{user}'.format(user=username),
            data=data
        )

        def response_handler(resp):
            if not resp.is_success:
                raise UserUpdateError(resp)
            return {
                'username': resp.body['user'],
                'active': resp.body['active'],
                'extra': resp.body['extra'],
            }

        return self._execute(request, response_handler)

    def replace_user(self, username, password, active=None, extra=None):
        """Replace an existing user.

        :param username: Username.
        :type username: str or unicode
        :param password: User's new password.
        :type password: str or unicode
        :param active: Whether the user is active.
        :type active: bool
        :param extra: Any extra data on the user.
        :type extra: dict
        :return: Details of the replaced user.
        :rtype: dict
        :raise arango.exceptions.UserReplaceError: If replace fails.
        """
        data = {'user': username, 'passwd': password}
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra

        request = Request(
            method='put',
            endpoint='/_api/user/{user}'.format(user=username),
            data=data
        )

        def response_handler(resp):
            if resp.is_success:
                return {
                    'username': resp.body['user'],
                    'active': resp.body['active'],
                    'extra': resp.body['extra'],
                }
            raise UserReplaceError(resp)

        return self._execute(request, response_handler)

    def delete_user(self, username, ignore_missing=False):
        """Delete an existing user.

        :param username: Username.
        :type username: str or unicode
        :param ignore_missing: Do not raise an exception on missing user.
        :type ignore_missing: bool
        :return: True if the operation was successful, False if the user was
            missing and **ignore_missing** was set to True.
        :rtype: bool
        :raise arango.exceptions.UserDeleteError: If delete fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/user/{user}'.format(user=username)
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            elif resp.status_code == 404 and ignore_missing:
                return False
            raise UserDeleteError(resp)

        return self._execute(request, response_handler)

    def permissions(self, username):
        """Return the user permissions for all databases and collections.

        :param username: Username.
        :type username: str or unicode
        :return: Permissions for all databases and collections.
        :rtype: dict
        :raise: arango.exceptions.PermissionGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/user/{}/database'.format(username),
            params={'full': True}
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body['result']
            raise PermissionGetError(resp)

        return self._execute(request, response_handler)

    def permission(self, username, database, collection=None):
        """Return the user permission for a specific database or collection.

        :param username: Username.
        :type username: str or unicode
        :param database: Database name.
        :type database: str or unicode
        :param collection: Collection name.
        :type collection: str or unicode
        :return: Permission for given database or collection.
        :rtype: str or unicode
        :raise: arango.exceptions.PermissionGetError: If retrieval fails.
        """
        endpoint = '/_api/user/{}/database/{}'.format(username, database)
        if collection is not None:
            endpoint += '/' + collection
        request = Request(method='get', endpoint=endpoint)

        def response_handler(resp):
            if not resp.is_success:
                raise PermissionGetError(resp)
            return resp.body['result']

        return self._execute(request, response_handler)

    def update_permission(self,
                          username,
                          permission,
                          database,
                          collection=None):
        """Update user permission for a specific database or collection.

        :param username: Username.
        :type username: str or unicode
        :param database: Database name.
        :type database: str or unicode
        :param collection: Collection name.
        :type collection: str or unicode
        :param permission: Allowed values are "rw" (read and write), "ro"
            (read only) or "none" (no access).
        :type permission: str or unicode
        :return: True if the access was successfully granted.
        :rtype: bool
        :raise arango.exceptions.PermissionUpdateError: If operation fails.
        """
        endpoint = '/_api/user/{}/database/{}'.format(username, database)
        if collection is not None:
            endpoint += '/' + collection

        request = Request(
            method='put',
            endpoint=endpoint,
            data={'grant': permission}
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            raise PermissionUpdateError(resp)

        return self._execute(request, response_handler)

    def delete_permission(self, username, database, collection=None):
        """Clear user permission for a specific database or collection.

        :param username: Username.
        :type username: str or unicode
        :param database: Database name.
        :type database: str or unicode
        :param collection: Collection name.
        :type collection: str or unicode
        :return: True if the permission was successfully cleared.
        :rtype: bool
        :raise arango.exceptions.PermissionDeleteError: If clear fails.
        """
        endpoint = '/_api/user/{}/database/{}'.format(username, database)
        if collection is not None:
            endpoint += '/' + collection
        request = Request(method='delete', endpoint=endpoint)

        def response_handler(resp):
            if resp.is_success:
                return True
            raise PermissionDeleteError(resp)

        return self._execute(request, response_handler)

    ########################
    # Async Job Management #
    ########################

    def async_jobs(self, status, count=None):
        """Return the IDs of asynchronous jobs with the given status.

        :param status: Job status ("pending" or "done").
        :type status: str or unicode
        :param count: Maximum number of job IDs to return.
        :type count: int
        :return: List of job IDs.
        :rtype: [str or unicode]
        :raise arango.exceptions.AsyncJobListError: If retrieval fails.
        """
        params = {}
        if count is not None:
            params['count'] = count

        request = Request(
            method='get',
            endpoint='/_api/job/{}'.format(status),
            params=params
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body
            raise AsyncJobListError(resp)

        return self._execute(request, response_handler)

    def clear_async_jobs(self, threshold=None):
        """Clear async job results from the server.

        :param threshold: If specified, only the job results created prior to
            the threshold (a unix timestamp) are deleted. Otherwise, all job
            results are deleted.
        :type threshold: int
        :return: True if the job result were deleted successfully.
        :rtype: bool
        :raise arango.exceptions.AsyncJobClearError: If delete fails.

        .. note::
            Async jobs currently queued or running are not stopped.
        """

        if threshold is None:
            url = '/_api/job/all'
            params = None
        else:
            url = '/_api/job/expired'
            params = {'stamp': threshold}

        request = Request(
            method='delete',
            endpoint=url,
            params=params
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            raise AsyncJobClearError(resp)

        return self._execute(request, response_handler)

    #########################
    # Pregel Job Management #
    #########################

    def pregel_job(self, job_id):
        """Return the details of a Pregel job.

        :param job_id: Pregel job ID.
        :type job_id: int
        :return: Details of the Pregel job.
        :rtype: dict
        :raise arango.exceptions.PregelJobGetError: If lookup fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/control_pregel/{}'.format(job_id)
        )

        def response_handler(resp):
            if not resp.is_success:
                raise PregelJobGetError(resp)
            if 'edgeCount' in resp.body:
                resp.body['edge_count'] = resp.body.pop('edgeCount')
            if 'receivedCount' in resp.body:
                resp.body['received_count'] = resp.body.pop('receivedCount')
            if 'sendCount' in resp.body:
                resp.body['send_count'] = resp.body.pop('sendCount')
            if 'totalRuntime' in resp.body:
                resp.body['total_runtime'] = resp.body.pop('totalRuntime')
            if 'vertexCount' in resp.body:
                resp.body['vertex_count'] = resp.body.pop('vertexCount')
            return resp.body

        return self._execute(request, response_handler)

    def create_pregel_job(self,
                          algorithm,
                          graph,
                          store=True,
                          max_gss=None,
                          thread_count=None,
                          async_mode=None,
                          result_field=None,
                          alg_params=None):
        """Start a new Pregel job.

        :param algorithm: Algorithm (e.g. "pagerank").
        :type algorithm: str or unicode
        :param graph: Graph name.
        :type graph: str or unicode
        :param store: If set to True, the Pregel engine writes results back to
            the database. If set to False, the results can be queried via AQL.
        :type store: bool
        :param max_gss: Maximum number of global iterations for the algorithm.
        :type max_gss: int
        :param thread_count: Number of parallel threads to use per worker.
            This does not influence the number of threads used to load or store
            data from the database (this depends on the number of shards).
        :type thread_count: int
        :param async_mode: Algorithms which support async mode run without
            synchronized global iterations. This might lead to performance
            increase if there are load imbalances.
        :type async_mode: bool
        :param result_field: If specified, most algorithms will write their
            results into the given field.
        :type result_field: str or unicode
        :param alg_params: Other algorithm parameters.
        :type alg_params: dict
        :return: Pregel job ID.
        :rtype: int
        :raise arango.exceptions.PregelJobCreateError: If create fails.
        """
        data = {
            'algorithm': algorithm,
            'graphName': graph,
        }

        algorithm_params = {}
        if store is not None:
            algorithm_params['store'] = store
        if max_gss is not None:
            algorithm_params['maxGSS'] = max_gss
        if thread_count is not None:
            algorithm_params['parallelism'] = thread_count
        if async_mode is not None:
            algorithm_params['async'] = async_mode
        if result_field is not None:
            algorithm_params['resultField'] = result_field
        if algorithm_params:
            data['params'] = alg_params

        request = Request(
            method='post',
            endpoint='/_api/control_pregel',
            data=data
        )

        def response_handler(resp):
            if resp.is_success:
                return resp.body
            raise PregelJobCreateError(resp)

        return self._execute(request, response_handler)

    def delete_pregel_job(self, job_id):
        """Cancel a Pregel job.

        :param job_id: Pregel job ID.
        :type job_id: int
        :return: True if the Pregel job was cancelled successfully.
        :rtype: bool
        :raise arango.exceptions.PregelJobDeleteError: If cancel fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/control_pregel/{}'.format(job_id)
        )

        def response_handler(resp):
            if resp.is_success:
                return True
            raise PregelJobDeleteError(resp)

        return self._execute(request, response_handler)
