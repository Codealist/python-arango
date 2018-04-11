from __future__ import absolute_import, unicode_literals

from arango.utils import split_id

__all__ = [
    'DefaultDatabase',
    'AsyncDatabase',
    'BatchDatabase',
    'TransactionDatabase'
]

from datetime import datetime

from arango.api import APIWrapper
from arango.aql import AQL
from arango.executor import (
    DefaultExecutor,
    AsyncExecutor,
    BatchExecutor,
    TransactionExecutor,
)
from arango.collection import DefaultCollection
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
    PermissionGetError,
    PermissionClearError,
    PermissionUpdateError,
    ServerConnectionError,
    ServerEndpointsError,
    ServerEngineError,
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
    UserCreateError,
    UserDeleteError,
    UserGetError,
    UserListError,
    UserReplaceError,
    UserUpdateError,
    DocumentParseError
)
from arango.foxx import Foxx
from arango.graph import Graph
from arango.pregel import Pregel
from arango.request import Request
from arango.wal import WAL


class Database(APIWrapper):
    """ArangoDB database base class.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.executor.DefaultExecutor
    """

    def __init__(self, connection, executor):
        super(Database, self).__init__(connection, executor)

    def __getitem__(self, name):
        """Return the collection wrapper.

        :param name: Collection name.
        :type name: str | unicode
        :return: Collection wrapper.
        :rtype: arango.collection.DefaultCollection
        """
        return self.collection(name)

    def _get_col_by_doc(self, document):
        """Return the collection for the given document.

        :param document: Document ID or body with "_id" field.
        :type document: str | unicode | dict
        :return: Collection wrapper.
        :rtype: arango.collection.DefaultCollection
        :raise arango.exceptions.DocumentParseError: On malformed document.
        """
        return self.collection(split_id(document)[0])

    def _get_col_by_docs(self, documents):
        """Return the collection for the given document.

        :param documents: List of document IDs or bodies with "_id" fields.
        :rtype: [str | unicode | dict]
        :return: Collection wrapper.
        :rtype: arango.collection.DefaultCollection
        :raise arango.exceptions.DocumentParseError: On malformed document.
        """
        if len(documents) == 0:
            raise DocumentParseError('got empty list of documents')
        return self._get_col_by_doc(documents[0])

    @property
    def name(self):
        """Return the database name.

        :return: Database name.
        :rtype: str | unicode
        """
        return self.db_name

    @property
    def aql(self):
        """Return the AQL wrapper used to execute AQL statements.

        See :class:`arango.aql.AQL` for more information.

        :return: AQL wrapper.
        :rtype: arango.aql.AQL
        """
        return AQL(self._conn, self._executor)

    @property
    def wal(self):
        """Return the WAL (write-ahead log) wrapper.

        See :class:`arango.wal.WAL` for more information.

        :return: WAL wrapper.
        :rtype: arango.wal.WAL
        """
        return WAL(self._conn, self._executor)

    @property
    def foxx(self):
        """Return the Foxx wrapper.

        See :class:`arango.foxx.Foxx` for more information.

        :return: Foxx wrapper.
        :rtype: arango.foxx.Foxx
        """
        return Foxx(self._conn, self._executor)

    @property
    def pregel(self):
        """Return the Pregel wrapper.

        See :class:`arango.pregel.Pregel` for more information.

        :return: Pregel wrapper.
        :rtype: arango.pregel.Pregel
        """
        return Pregel(self._conn, self._executor)

    def properties(self):
        """Return the database properties.

        :return: Database properties.
        :rtype: dict
        :raise arango.exceptions.DatabasePropertiesError: If retrieval fails.
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

    def execute_transaction(self,
                            command,
                            params=None,
                            read=None,
                            write=None,
                            sync=None,
                            timeout=None,
                            max_size=None,
                            allow_implicit=None,
                            intermediate_commit_count=None,
                            intermediate_commit_size=None):
        """Execute a raw Javascript code in a transaction.

        :param command: Javascript code to execute.
        :type command: str | unicode
        :param read: Names of collections read during transaction.
            If **allow_implicit** is set to True, any undeclared collections
            are loaded lazily.
        :type read: [str | unicode]
        :param write: Names of collections where data is written
            during transaction. Transaction fails on undeclared collections.
        :type write: [str | unicode]
        :param params: Optional arguments passed to **action**.
        :type params: dict
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param timeout: Timeout for waiting on collection locks. If set
            to 0, the ArangoDB server waits indefinitely. If not set, system
            default value is used.
        :type timeout: int
        :param max_size: Max transaction size limit in bytes. Applies only
            to RocksDB storage engine.
        :type max_size: int
        :param allow_implicit: If set to True, undeclared read collections are
            loaded lazily. If set to False, transaction fails on undeclared
            collections.
        :type allow_implicit: bool
        :param intermediate_commit_count: Max number of operations after
            which an intermediate commit is performed automatically. Applies
            only to RocksDB storage engine.
        :type intermediate_commit_count: int
        :param intermediate_commit_size: Max size of operations in bytes
            after which an intermediate commit is performed automatically.
            Applies only to RocksDB storage engine.
        :type intermediate_commit_size: int
        :return: Return value of the code defined in **action**.
        :rtype: str | unicode
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
        if intermediate_commit_count is not None:
            data['intermediateCommitCount'] = intermediate_commit_count
        if intermediate_commit_size is not None:
            data['intermediateCommitSize'] = intermediate_commit_size

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
        :rtype: str | unicode
        :raise arango.exceptions.ServerVersionError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/version',
            params={'details': False}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerVersionError(resp)
            return resp.body['version']

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
            params={'details': True}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise ServerDetailsError(resp)
            return resp.body['details']

        return self._execute(request, response_handler)

    def target_version(self):
        """Return required version of target database.

        :return: Required version of target database.
        :rtype: str | unicode
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

    def endpoints(self):  # pragma: no cover
        """Return the information about all coordinate endpoints.

        :return: List of endpoints.
        :rtype: [str | unicode]
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
        :rtype: str | unicode
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
                raise ServerConnectionError('bad username and/or password')
            if not resp.is_success:
                raise ServerConnectionError(
                    resp.error_message or 'bad server response')
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
        :rtype: str | unicode
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
        :raise arango.exceptions.ServerEchoError: If retrieval fails.
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
        :type tests: [str | unicode]
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
        :type upto: str | unicode | int
        :param level: return the log entries of only the given level (mutually
            exclusive with **upto**), which must be "fatal", "error",
            "warning", "info" (default) or "debug"
        :type level: str | unicode | int
        :param start: return the log entries whose ID is greater or equal to
            the given value
        :type start: int
        :param size: restrict the size of the result to the given value (this
            setting can be used for pagination)
        :type size: int
        :param offset: Number of entries to skip (e.g. for pagination).
        :type offset: int
        :param search: return only the log entries containing the given text
        :type search: str | unicode
        :param sort: sort the log entries according to the given fashion, which
            can be "sort" or "desc"
        :type sort: str | unicode
        :return: Server log entries
        :rtype: dict
        :raise arango.exceptions.ServerReadLogError: If read fails.
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
        :raise arango.exceptions.ServerReloadRoutingError: If reload fails.
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
            endpoint='/_api/database'
        )

        def response_handler(resp):
            if not resp.is_success:
                raise DatabaseListError(resp)
            return resp.body['result']

        return self._execute(request, response_handler)

    def create_database(self, name, users=None):
        """Create a new database.

        :param name: Database name.
        :type name: str | unicode
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
        :rtype: arango.database.DefaultDatabase
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

    def delete_database(self, name):
        """Delete the database of the specified name.

        :param name: Database name.
        :type name: str | unicode
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
                raise DatabaseDeleteError(resp)
            return resp.body['result']

        return self._execute(request, response_handler)

    #########################
    # Collection Management #
    #########################

    def collection(self, name):
        """Return the collection wrapper.

        :param name: Collection name.
        :type name: str | unicode
        :return: Collection wrapper.
        :rtype: arango.collection.DefaultCollection
        """
        return DefaultCollection(self._conn, self._executor, name)

    def collections(self):
        """Return the collections in the database.

        :return: Details of the collections in the database.
        :rtype: [dict] | [str | unicode]
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
                'type': DefaultCollection.types[col['type']],
                'status': DefaultCollection.statuses[col['status']],
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
        :type name: str | unicode
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param compact: Whether the collection is compacted.
        :type compact: bool
        :param system: Whether the collection is a system collection. Starting
            from ArangoDB version 3.1+, system collections must have a name
            with a leading underscore "_" character.
        :type system: bool
        :param journal_size: Max size of the journal in bytes.
        :type journal_size: int
        :param edge: Whether the collection is an edge collection.
        :type edge: bool
        :param volatile: Whether The collection is in-memory only.
        :type volatile: bool
        :param key_generator: Used for generating document keys. Allowed values
            are "traditional" or "autoincrement".
        :type key_generator: str | unicode
        :param user_keys: Whether to allow users to supply the document keys.
        :type user_keys: bool
        :param key_increment: Key increment value. Applies only when the value
            of **key_generator** is set to "autoincrement".
        :type key_increment: int
        :param key_offset: Key offset value. Applies only when the value of
            **key_generator** is set to "autoincrement".
        :type key_offset: int
        :param shard_fields: Field(s) used to determine the target shard.
        :type shard_fields: [str | unicode]
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
        :rtype: arango.collection.DefaultCollection
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
        :type name: str | unicode
        :param ignore_missing: Do not raise an exception on missing collection.
        :type ignore_missing: bool
        :param system: Whether the collection is a system collection.
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
            if resp.error_code == 1203 and ignore_missing:
                return False
            if not resp.is_success:
                raise CollectionDeleteError(resp)
            return True

        return self._execute(request, response_handler)

    ####################
    # Graph Management #
    ####################

    def graph(self, name):
        """Return the graph wrapper.

        :param name: Graph name.
        :type name: str | unicode
        :return: Graph wrapper.
        :rtype: arango.graph.Graph
        """
        return Graph(self._conn, self._executor, name)

    def graphs(self):
        """List all graphs in the database.

        :return: Graphs in the database.
        :rtype: dict | [str | unicode]
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
        :type name: str | unicode
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
        :type orphan_collections: [str | unicode]
        :param smart: If set to True, sharding is enabled (see parameter
            **smart_field** below). This is only for the enterprise version of
            ArangoDB.
        :type smart: bool
        :param smart_field: Document field used to shard the vertices of
            the graph. To use this, parameter **smart** must be set to True and
            every vertex in the graph must have the smart field. This is only
            for the enterprise version of ArangoDB.
        :type smart_field: str | unicode
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
        :type name: str | unicode
        :param ignore_missing: Do not raise an exception on missing graph.
        :type ignore_missing: bool
        :param drop_collections: Drop the collections of the graph as well
            (only if they are not in use by other graphs).
        :type drop_collections: bool
        :return: True if deletion was successful. False if graph was missing
            and **ignore_missing** was set to True.
        :rtype: bool
        :raise arango.exceptions.GraphDeleteError: If delete fails.
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
            if resp.error_code == 1924 and ignore_missing:
                return False
            if not resp.is_success:
                raise GraphDeleteError(resp)
            return True

        return self._execute(request, response_handler)

    #######################
    # Document Management #
    #######################

    def document(self, document, rev=None, check_rev=True):
        """Return a document.

        :param document: Document ID or body with "_id" field.
        :type document: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **document** if any.
        :type rev: str | unicode
        :param check_rev: If set to True, the revision of **document** (if any)
            is compared against the revision of the target document.
        :type check_rev: bool
        :return: Document or None if not found.
        :rtype: dict | None
        :raise arango.exceptions.DocumentGetError: If retrieval fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_doc(document).get(
            document=document,
            rev=rev,
            check_rev=check_rev
        )

    def documents(self, documents):
        """Return multiple documents ignoring any missing ones.

        :param documents: List of document IDs or bodies with "_id" fields.
        :type documents: [str | unicode | dict]
        :return: Documents. Missing ones are not included.
        :rtype: [dict]
        :raise arango.exceptions.DocumentGetError: If retrieval fails.

        .. note::
            The ID of the first document in **documents** is used to determine
            the target collection name.
        """
        return self._get_col_by_docs(documents).get_many(documents)

    def insert_document(self,
                        collection,
                        document,
                        return_new=False,
                        sync=None,
                        silent=False):
        """Insert a new document.

        :param collection: Collection name.
        :type collection: str | unicode
        :param document: Document to insert. If it contains the "_key" field,
            the value is used as the key of the new document (auto-generated
            otherwise). Any "_id" or "_rev" field is ignored.
        :type document: dict
        :param return_new: Include body of the new document in the returned
            metadata. Ignored if parameter **silent** is set to True.
        :type return_new: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentInsertError: If insert fails.
        """
        return self.collection(collection).insert(
            document=document,
            return_new=return_new,
            sync=sync,
            silent=silent
        )

    def insert_documents(self,
                         collection,
                         documents,
                         return_new=False,
                         sync=None,
                         silent=False):
        """Insert multiple documents into the collection.

        :param collection: Target collection name.
        :type collection: str | unicode
        :param documents: List of new documents to insert. If they contain the
            "_key" fields, the values are used as the keys of the new documents
            (auto-generated otherwise). Any "_id" or "_rev" field is ignored.
        :type documents: [dict]
        :param return_new: Include bodies of the new documents in the returned
            metadata. Ignored if parameter **silent** is set to True
        :type return_new: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | ArangoError] | bool
        :raise arango.exceptions.DocumentInsertError: If insert fails.

        .. note::
            If inserting a document fails, the exception object is placed in
            the result list instead of document metadata.

        .. warning::
            Parameters **return_new** should be used with caution, as the size
            of returned result brought into client-side memory scales with the
            number of documents inserted.
        """
        return self.collection(collection).insert_many(
            documents=documents,
            return_new=return_new,
            sync=sync,
            silent=silent
        )

    def update_document(self,
                        document,
                        check_rev=True,
                        merge=True,
                        keep_none=True,
                        return_new=False,
                        return_old=False,
                        sync=None,
                        silent=False):
        """Update a document.

        :param document: Partial or full document with the updated values. It
            must contain the "_id" field.
        :type document: dict
        :param check_rev: If set to True, the "_rev" field in **document** (if
            present) is compared against the revision of the target document.
        :type check_rev: bool
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new one overwriting the old one.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: Include body of the new document in the result.
        :type return_new: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentUpdateError: If update fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_doc(document).update(
            document=document,
            check_rev=check_rev,
            merge=merge,
            keep_none=keep_none,
            return_new=return_new,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def update_documents(self,
                         documents,
                         check_rev=True,
                         merge=True,
                         keep_none=True,
                         return_new=False,
                         return_old=False,
                         sync=None,
                         silent=False):
        """Update multiple documents.

        :param documents: Partial or full documents with the updated values.
            They must contain the "_id" fields.
        :type documents: [dict]
        :param check_rev: If set to True, the "_rev" fields in **documents**
            (if any) are compared against the revisions of target documents.
        :type check_rev: bool
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new ones overwriting the old ones.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | ArangoError] | bool
        :raise arango.exceptions.DocumentUpdateError: If update fails.

        .. note::
            The ID of the first document in **documents** is used to determine
            the target collection name.

        .. note::
            If updating a document fails, the exception object is placed in
            the result list instead of document metadata.

        .. warning::
            Parameters **return_new** and **return_old** should be used with
            caution, as the size of returned result brought into client-side
            memory scales with the number of documents updated.
        """
        return self._get_col_by_docs(documents).update_many(
            documents=documents,
            check_rev=check_rev,
            merge=merge,
            keep_none=keep_none,
            return_new=return_new,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def replace_document(self,
                         document,
                         check_rev=True,
                         return_new=False,
                         return_old=False,
                         sync=None,
                         silent=False):
        """Replace a document.

        :param document: New document to replace the old one with. It must
            contain the "_id" field. Edge document must also have "_from"
            and "_to" fields.
        :type document: dict
        :param check_rev: If set to True, the "_rev" field in **document**
            is compared against the revision of the target document.
        :type check_rev: bool
        :param return_new: Include body of the new document in the result.
        :type return_new: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentReplaceError: If replace fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_doc(document).replace(
            document=document,
            check_rev=check_rev,
            return_new=return_new,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def replace_documents(self,
                          documents,
                          check_rev=True,
                          return_new=False,
                          return_old=False,
                          sync=None,
                          silent=False):
        """Replace multiple documents.

        :param documents: New documents to replace the old ones with. They must
            contain the "_id" fields. Edge documents must also have "_from" and
            "_to" fields.
        :type documents: [dict]
        :param check_rev: If set to True, the "_rev" fields in **documents**
            (if any) are compared against the revisions of target documents.
        :type check_rev: bool
        :param return_new: Include bodies of the new documents in the result.
        :type return_new: bool
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: List of document metadata (e.g. document keys, revisions) and
            any exceptions, or True if parameter **silent** was set to True.
        :rtype: [dict | ArangoError] | bool
        :raise arango.exceptions.DocumentReplaceError: If replace fails.

        .. note::
            The ID of the first document in **documents** is used to determine
            the target collection name.

        .. note::
            If replacing a document fails, the exception object is placed in
            the result list instead of document metadata.

        .. warning::
            Parameters **return_new** and **return_old** should be used with
            caution, as the size of returned result brought into client-side
            memory scales with the number of documents replaced.
        """
        return self._get_col_by_docs(documents).replace_many(
            documents=documents,
            check_rev=check_rev,
            return_new=return_new,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def delete_document(self,
                        document,
                        rev=None,
                        check_rev=True,
                        ignore_missing=False,
                        return_old=False,
                        sync=None,
                        silent=False):
        """Delete a document.

        :param document: Document ID, key or body. Document body must contain
            the "_id" field.
        :type document: str | unicode | dict
        :param rev: Expected document revision. Overrides the value of "_rev"
            field in **document** if any.
        :type rev: str | unicode
        :param check_rev: If set to True, the revision of **document** (if any)
            is compared against the revision of the target document.
        :type check_rev: bool
        :param ignore_missing: Do not raise an exception on missing document.
            This parameter has no effect in transactions where an exception is
            always raised.
        :type ignore_missing: bool
        :param return_old: Include body of the old document in the result.
        :type return_old: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document key, revision), or True if
            parameter **silent** was set to True, or False if document is
            missing and **ignore_missing** was set to True (does not apply
            in transactions).
        :rtype: bool | dict
        :raise arango.exceptions.DocumentDeleteError: If delete fails.
        :raise arango.exceptions.DocumentRevisionError: If revisions mismatch.
        """
        return self._get_col_by_doc(document).delete(
            document=document,
            rev=rev,
            check_rev=check_rev,
            ignore_missing=ignore_missing,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

    def delete_documents(self,
                         documents,
                         check_rev=True,
                         return_old=False,
                         sync=None,
                         silent=False):
        """Delete multiple documents.

        :param documents: Document IDs or bodies with "_id" fields.
        :type documents: [str | unicode | dict]
        :param return_old: Include bodies of the old documents in the result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" fields in **documents**
            are compared against the revisions of the target documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no document metadata is returned. This
            can be used to save resources.
        :type silent: bool
        :return: Document metadata (e.g. document keys, revisions) or True if
            parameter **silent** was set to True.
        :rtype: bool | dict
        :raise arango.exceptions.DocumentDeleteError: If delete fails.

        .. note::
            The ID of the first document in **documents** is used to determine
            the target collection name.

        .. note::
            The ID of the first document in **documents** is used to determine
            the target collection name.

        .. warning::
            Parameters **return_old** should be used with caution, as the size
            of returned metadata (brought into client-side memory) scales with
            the number of documents deleted.
        """
        return self._get_col_by_docs(documents).delete_many(
            documents=documents,
            check_rev=check_rev,
            return_old=return_old,
            sync=sync,
            silent=silent
        )

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
        :type task_id: str | unicode
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

    def create_task(self,
                    name,
                    command,
                    params=None,
                    period=None,
                    offset=None,
                    task_id=None):
        """Create a new server task.

        :param name: Name of the server task.
        :type name: str | unicode
        :param command: Javascript code to execute.
        :type command: str | unicode
        :param params: Parameters passed into the command.
        :type params: dict
        :param period: Number of seconds to wait between executions. If set
            to 0, the new task will be "timed", meaning it will execute only
            once and be deleted automatically afterwards.
        :type period: int
        :param offset: Initial delay before execution in seconds.
        :type offset: int
        :param task_id: Pre-defined ID for the new server task.
        :type task_id: str | unicode
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
        :type task_id: str | unicode
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
            if resp.error_code == 1852 and ignore_missing:
                return False
            if not resp.is_success:
                raise TaskDeleteError(resp)
            return True

        return self._execute(request, response_handler)

    ###################
    # User Management #
    ###################

    def users(self):
        """Return the details of all users.

        :return: Details of all users or just the usernames.
        :rtype: [str | unicode | dict]
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
        :type username: str | unicode
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
        :type username: str | unicode
        :param password: Password.
        :type password: str | unicode
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
        :type username: str | unicode
        :param password: New password.
        :type password: str | unicode
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
        :type username: str | unicode
        :param password: User's new password.
        :type password: str | unicode
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
        :type username: str | unicode
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
        :type username: str | unicode
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
        :type username: str | unicode
        :param database: Database name.
        :type database: str | unicode
        :param collection: Collection name.
        :type collection: str | unicode
        :return: Permission for given database or collection.
        :rtype: str | unicode
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
        """Update user permission for given database/collection.

        :param username: Username.
        :type username: str | unicode
        :param database: Database name.
        :type database: str | unicode
        :param collection: Collection name.
        :type collection: str | unicode
        :param permission: Allowed values are "rw" (read and write), "ro"
            (read only) or "none" (no access).
        :type permission: str | unicode
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

    def clear_permission(self, username, database, collection=None):
        """Clear user permission for given database/collection.

        The user permission is reset back to the default value (e.g. fallback
        to database-level permission).

        :param username: Username.
        :type username: str | unicode
        :param database: Database name.
        :type database: str | unicode
        :param collection: Collection name.
        :type collection: str | unicode
        :return: True if the permission was successfully cleared.
        :rtype: bool
        :raise arango.exceptions.PermissionClearError: If clear fails.
        """
        endpoint = '/_api/user/{}/database/{}'.format(username, database)
        if collection is not None:
            endpoint += '/' + collection
        request = Request(method='delete', endpoint=endpoint)

        def response_handler(resp):
            if resp.is_success:
                return True
            raise PermissionClearError(resp)

        return self._execute(request, response_handler)

    ########################
    # Async Job Management #
    ########################

    def async_jobs(self, status, count=None):
        """Return the IDs of asynchronous jobs with the given status.

        :param status: Job status ("pending" or "done").
        :type status: str | unicode
        :param count: Max number of job IDs to return.
        :type count: int
        :return: List of job IDs.
        :rtype: [str | unicode]
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


class DefaultDatabase(Database):
    """Database wrapper.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    """

    def __init__(self, connection):
        super(DefaultDatabase, self).__init__(
            connection=connection,
            executor=DefaultExecutor(connection)
        )

    def __repr__(self):
        return '<DefaultDatabase {}>'.format(self.name)

    def begin_async(self, return_result=True):
        """Begin async API execution.

        :param return_result: If set to True, API execution results are saved
            server-side and instances of :class:`arango.job.AsyncJob` are
            returned. If set to False, API executions return None and results
            are not saved server-side.
        :type return_result: bool
        :return: New database wrapper. API requests made using this database
            are queued up server-side and executed asynchronously.
        :rtype: arango.database.AsyncDatabase
        """
        return AsyncDatabase(self._conn, return_result)

    def begin_batch(self, return_result=True):
        """Begin batch API execution.

        :param return_result: If set to True, API executions return instances
            of :class:`arango.job.BatchJob`. The job instances are populated
            with results on commit. If set to False, API executions return None
            instead and no results are saved client-side.
        :type return_result: bool
        :return: New database wrapper. API requests made using this database
            are queued up client-side and executed in a batch on commit.
        :rtype: arango.database.BatchDatabase
        """
        return BatchDatabase(self._conn, return_result)

    def begin_transaction(self,
                          return_result=True,
                          timeout=None,
                          sync=None,
                          read=None,
                          write=None):
        """Begin transaction.

        :param return_result: If set to True, API executions return instances
            of :class:`arango.job.TransactionJob`. These job instances
            are populated with results on commit. If set to False, executions
            return None instead and no results are saved client-side.
        :type return_result: bool
        :param read: Names of collections read during the transaction. If not
            specified, they are added automatically as jobs are queued.
        :type read: [str | unicode]
        :param write: Names of collections written to during the transaction.
            If not specified, they are added automatically as jobs are queued.
        :type write: [str | unicode]
        :param timeout: Collection lock timeout.
        :type timeout: int
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: New database wrapper. API requests made using this database
            are queued up client-side and executed in a transaction on commit.
        :rtype: arango.database.TransactionDatabase
        """
        return TransactionDatabase(
            connection=self._conn,
            return_result=return_result,
            read=read,
            write=write,
            timeout=timeout,
            sync=sync
        )


class AsyncDatabase(Database):
    """Database wrapper specifically for async API executions.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, API execution results are saved
        server-side and instances of :class:`arango.job.AsyncJob` are returned.
        If set to False, API executions return None and results are not saved
        server-side.
    :type return_result: bool
    """

    def __init__(self, connection, return_result):
        super(AsyncDatabase, self).__init__(
            connection=connection,
            executor=AsyncExecutor(connection, return_result)
        )

    def __repr__(self):
        return '<AsyncDatabase {}>'.format(self.name)


class BatchDatabase(Database):
    """Database wrapper specifically for batch API executions.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, API executions return instances
        of :class:`arango.job.BatchJob`. The job instances are populated with
        results on commit. If set to False, API executions return None instead
        and no results are saved client-side.
    :type return_result: bool
    """

    def __init__(self, connection, return_result):
        super(BatchDatabase, self).__init__(
            connection=connection,
            executor=BatchExecutor(connection, return_result)
        )

    def __repr__(self):
        return '<BatchDatabase {}>'.format(self.name)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None:
            self._executor.commit()

    def queued_jobs(self):
        """Return the queued batch jobs.

        :return: Batch jobs or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [arango.job.BatchJob] | None
        """
        return self._executor.jobs

    def commit(self):
        """Execute the queued requests in a single batch API request.

        If **return_result** parameter was set to True during initialization,
        :class:`arango.job.BatchJob` instances are populated with results.

        :return: Batch jobs or None if **return_result** parameter was set to
            False during initialization.
        :rtype: [arango.job.BatchJob] | None
        :raise arango.exceptions.BatchStateError: If batch state is invalid
            (e.g. batch was already committed or the response size did not
            match the expected).
        :raise arango.exceptions.BatchExecuteError: If commit fails.
        """
        return self._executor.commit()


class TransactionDatabase(Database):
    """Database wrapper specifically for transactions.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param return_result: If set to True, API executions return instances
        of :class:`arango.job.TransactionJob`. These job instances are
        populated with results on commit. If set to False, executions return
        None instead and no results are saved client-side.
    :type return_result: bool
    :param read: Names of collections read during the transaction.
    :type read: [str | unicode]
    :param write: Names of collections written to during the transaction.
    :type write: [str | unicode]
    :param timeout: Collection lock timeout.
    :type timeout: int
    :param sync: Block until the operation is synchronized to disk.
    :type sync: bool
    """

    def __init__(self, connection, return_result, read, write, timeout, sync):
        super(TransactionDatabase, self).__init__(
            connection=connection,
            executor=TransactionExecutor(
                connection=connection,
                return_result=return_result,
                read=read,
                write=write,
                timeout=timeout,
                sync=sync
            )
        )

    def __repr__(self):
        return '<TransactionDatabase {}>'.format(self.name)

    def __enter__(self):
        return self

    def __exit__(self, exception, *_):
        if exception is None:
            self._executor.commit()

    def queued_jobs(self):
        """Return the queued transaction jobs.

        :return: Transaction jobs or None if **return_result** parameter was
            set to False during initialization.
        :rtype: [arango.job.TransactionJob] | None
        """
        return self._executor.jobs

    def commit(self):
        """Execute the queued requests in a single transaction API request.

        If **return_result** parameter was set to True during initialization,
        :class:`arango.job.TransactionJob` instances are populated with
        results.

        :return: Transaction jobs or None if **return_result** parameter was
            set to False during initialization.
        :rtype: [arango.job.TransactionJob] | None
        :raise arango.exceptions.TransactionStateError: If the transaction was
            already committed.
        :raise arango.exceptions.TransactionExecuteError: If commit fails.
        """
        return self._executor.commit()
