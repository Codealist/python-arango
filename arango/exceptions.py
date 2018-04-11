from __future__ import absolute_import, unicode_literals

from six import string_types


class ArangoError(Exception):
    """Base exception class."""


class ArangoClientError(ArangoError):
    """Encapsulates errors coming from python-arango client."""


class ArangoServerError(ArangoError):
    """Encapsulates errors coming from ArangoDB server.

    :param data: HTTP response or error message.
    :type data: str | unicode | arango.response.Response
    :param msg: Optional error message (overrides the default).
    :type msg: str | unicode

    :ivar message: Error message.
    :var_type message: str | unicode
    :ivar http_method: HTTP method (e.g. "post").
    :var_type http_method: str | unicode
    :ivar url: Request URL.
    :var_type url: str | unicode
    :ivar http_code: HTTP status code.
    :var_type http_code: int
    :ivar http_headers: Response headers.
    :var_type http_headers: dict
    :ivar error_code: ArangoDB error code.
    :var_type error_code: int
    """

    def __init__(self, resp=None, msg=None):
        msg = msg or resp.error_message or resp.status_text
        self.error_code = resp.error_code
        if self.error_code is not None:
            msg = '[HTTP {}][ERR {}] {}'.format(
                resp.status_code, self.error_code, msg)
        else:
            msg = '[HTTP {}] {}'.format(resp.status_code, msg)
        super(ArangoServerError, self).__init__(msg)
        self.message = msg
        self.http_method = resp.method
        self.url = resp.url
        self.http_code = resp.status_code
        self.http_headers = resp.headers


##################
# AQL Exceptions #
##################


class AQLQueryExplainError(ArangoServerError):
    """Failed to explain an AQL query."""


class AQLQueryValidateError(ArangoServerError):
    """Failed to validate an AQL query."""


class AQLQueryExecuteError(ArangoServerError):
    """Failed to execute an AQL query."""


class AQLQueryListError(ArangoServerError):
    """Failed to retrieve running AQL queries."""


class AQLQueryClearError(ArangoServerError):
    """Failed to clear slow AQL queries."""


class AQLQueryTrackingGetError(ArangoServerError):
    """Failed to retrieve AQL tracking properties."""


class AQLQueryTrackingSetError(ArangoServerError):
    """Failed to configure AQL tracking properties."""


class AQLQueryKillError(ArangoServerError):
    """Failed to kill an AQL query."""


class AQLCacheClearError(ArangoServerError):
    """Failed to clear the AQL query cache."""


class AQLCachePropertiesError(ArangoServerError):
    """Failed to retrieve the AQL query cache properties."""


class AQLCacheConfigureError(ArangoServerError):
    """Failed to configure the AQL query cache properties."""


class AQLFunctionListError(ArangoServerError):
    """Failed to list AQL user functions."""


class AQLFunctionCreateError(ArangoServerError):
    """Failed to create an AQL user function."""


class AQLFunctionDeleteError(ArangoServerError):
    """Failed to delete an AQL user function."""


####################
# Async Exceptions #
####################


class AsyncExecuteError(ArangoServerError):
    """Failed to execute asynchronous API."""


class AsyncJobListError(ArangoServerError):
    """Failed to list asynchronous jobs."""


class AsyncJobCancelError(ArangoServerError):
    """Failed to cancel asynchronous job."""


class AsyncJobStatusError(ArangoServerError):
    """Failed to retrieve asynchronous job status."""


class AsyncJobResultError(ArangoServerError):
    """Failed to retrieve asynchronous job result."""


class AsyncJobClearError(ArangoServerError):
    """Failed to delete asynchronous job result."""


####################
# Batch Exceptions #
####################


class BatchStateError(ArangoClientError):
    """The batch was in bad state and not executable."""


class BatchJobResultError(ArangoClientError):
    """Failed to retrieve batch job result."""


class BatchExecuteError(ArangoServerError):
    """Failed to execute batch API."""


#########################
# Collection Exceptions #
#########################


class CollectionListError(ArangoServerError):
    """Failed to list collections."""


class CollectionPropertiesError(ArangoServerError):
    """Failed to retrieve collection properties."""


class CollectionConfigureError(ArangoServerError):
    """Failed to configure collection properties."""


class CollectionStatisticsError(ArangoServerError):
    """Failed to retrieve collection statistics."""


class CollectionRevisionError(ArangoServerError):
    """Failed to retrieve collection revision."""


class CollectionChecksumError(ArangoServerError):
    """Failed to retrieve collection checksum."""


class CollectionCreateError(ArangoServerError):
    """Failed to create collection."""


class CollectionDeleteError(ArangoServerError):
    """Failed to delete collection."""


class CollectionRenameError(ArangoServerError):
    """Failed to rename collection."""


class CollectionTruncateError(ArangoServerError):
    """Failed to truncate collection."""


class CollectionLoadError(ArangoServerError):
    """Failed to load collection in memory."""


class CollectionUnloadError(ArangoServerError):
    """Failed to unload collection in memory."""


class CollectionRotateJournalError(ArangoServerError):
    """Failed to rotate the journal of the collection."""


#####################
# Cursor Exceptions #
#####################


class CursorNextError(ArangoServerError):
    """Failed to retrieve the next cursor result."""


class CursorCloseError(ArangoServerError):
    """Failed to delete the cursor from the server."""


#######################
# Database Exceptions #
#######################


class DatabaseListError(ArangoServerError):
    """Failed to retrieve the list of databases."""


class DatabasePropertiesError(ArangoServerError):
    """Failed to retrieve the database options."""


class DatabaseCreateError(ArangoServerError):
    """Failed to create the database."""


class DatabaseDeleteError(ArangoServerError):
    """Failed to delete the database."""


#######################
# Document Exceptions #
#######################

class DocumentParseError(ArangoClientError):
    """Failed to retrieve the document key."""


class DocumentCountError(ArangoServerError):
    """Failed to retrieve the count of the documents in the collections."""


class DocumentInError(ArangoServerError):
    """Failed to check whether a collection contains a document."""


class DocumentGetError(ArangoServerError):
    """Failed to retrieve the document."""


class DocumentKeysError(ArangoServerError):
    """Failed to retrieve the document keys."""


class DocumentIDsError(ArangoServerError):
    """Failed to retrieve the document IDs."""


class DocumentInsertError(ArangoServerError):
    """Failed to insert the document."""


class DocumentReplaceError(ArangoServerError):
    """Failed to replace the document."""


class DocumentUpdateError(ArangoServerError):
    """Failed to update the document."""


class DocumentDeleteError(ArangoServerError):
    """Failed to delete the document."""


class DocumentRevisionError(ArangoServerError):
    """The expected and actual document revisions do not match."""


###################
# Foxx Exceptions #
###################


class FoxxServiceListError(ArangoServerError):
    """Failed to list Foxx services."""


class FoxxServiceGetError(ArangoServerError):
    """Failed to retrieve Foxx service metadata."""


class FoxxServiceCreateError(ArangoServerError):
    """Failed to create a Foxx service."""


class FoxxServiceUpdateError(ArangoServerError):
    """Failed to update a Foxx service."""


class FoxxServiceReplaceError(ArangoServerError):
    """Failed to replace a Foxx service."""


class FoxxServiceDeleteError(ArangoServerError):
    """Failed to delete a Foxx services."""


class FoxxConfigGetError(ArangoServerError):
    """Failed to retrieve Foxx service configuration."""


class FoxxConfigUpdateError(ArangoServerError):
    """Failed to update Foxx service configuration."""


class FoxxConfigReplaceError(ArangoServerError):
    """Failed to replace Foxx service configuration."""


class FoxxDependencyGetError(ArangoServerError):
    """Failed to retrieve Foxx service dependencies."""


class FoxxDependencyUpdateError(ArangoServerError):
    """Failed to update Foxx service dependencies."""


class FoxxDependencyReplaceError(ArangoServerError):
    """Failed to replace Foxx service dependencies."""


class FoxxScriptListError(ArangoServerError):
    """Failed to list Foxx service scripts."""


class FoxxScriptRunError(ArangoServerError):
    """Failed to run Foxx service script."""


class FoxxTestRunError(ArangoServerError):
    """Failed to run Foxx service tests."""


class FoxxDevEnableError(ArangoServerError):
    """Failed to enable development mode for a service."""


class FoxxDevDisableError(ArangoServerError):
    """Failed to disable development mode for a service."""


class FoxxReadmeGetError(ArangoServerError):
    """Failed to retrieve service readme."""


class FoxxSwaggerGetError(ArangoServerError):
    """Failed to retrieve swagger description."""


class FoxxDownloadError(ArangoServerError):
    """Failed to download service bundle."""


class FoxxCommitError(ArangoServerError):
    """Failed to commit local service state."""


####################
# Graph Exceptions #
####################


class GraphListError(ArangoServerError):
    """Failed to retrieve the list of graphs."""


class GraphGetError(ArangoServerError):
    """Failed to retrieve the graph."""


class GraphCreateError(ArangoServerError):
    """Failed to create the graph."""


class GraphDeleteError(ArangoServerError):
    """Failed to delete the graph."""


class GraphPropertiesError(ArangoServerError):
    """Failed to retrieve the graph properties."""


class GraphTraverseError(ArangoServerError):
    """Failed to execute the graph traversal."""


class OrphanCollectionListError(ArangoServerError):
    """Failed to retrieve the list of orphan vertex collections."""


class VertexCollectionListError(ArangoServerError):
    """Failed to retrieve the list of vertex collections."""


class VertexCollectionCreateError(ArangoServerError):
    """Failed to create the vertex collection."""


class VertexCollectionDeleteError(ArangoServerError):
    """Failed to delete the vertex collection."""


class EdgeDefinitionListError(ArangoServerError):
    """Failed to retrieve the list of edge definitions."""


class EdgeDefinitionCreateError(ArangoServerError):
    """Failed to create the edge definition."""


class EdgeDefinitionReplaceError(ArangoServerError):
    """Failed to replace the edge definition."""


class EdgeDefinitionDeleteError(ArangoServerError):
    """Failed to delete the edge definition."""


class EdgeListError(ArangoServerError):
    """Failed to retrieve edges coming in and out of a vertex."""


####################
# Index Exceptions #
####################


class IndexListError(ArangoServerError):
    """Failed to retrieve the list of indexes in the collection."""


class IndexCreateError(ArangoServerError):
    """Failed to create the index in the collection."""


class IndexDeleteError(ArangoServerError):
    """Failed to delete the index from the collection."""


class IndexLoadError(ArangoServerError):
    """Failed to load the indexes into memory."""


#####################
# Pregel Exceptions #
#####################


class PregelJobCreateError(ArangoServerError):
    """Failed to create a Pregel job."""


class PregelJobGetError(ArangoServerError):
    """Failed to retrieve a Pregel job."""


class PregelJobDeleteError(ArangoServerError):
    """Failed to cancel a Pregel job."""


#####################
# Server Exceptions #
#####################


class ServerConnectionError(ArangoClientError):
    """Failed to connect to ArangoDB server."""


class ServerEngineError(ArangoServerError):
    """Failed to retrieve the database engine."""


class ServerEndpointsError(ArangoServerError):
    """Failed to retrieve the ArangoDB server endpoints."""


class ServerVersionError(ArangoServerError):
    """Failed to retrieve the ArangoDB server version."""


class ServerDetailsError(ArangoServerError):
    """Failed to retrieve the ArangoDB server details."""


class ServerTimeError(ArangoServerError):
    """Failed to return the current ArangoDB system time."""


class ServerEchoError(ArangoServerError):
    """Failed to return the last request."""


class ServerShutdownError(ArangoServerError):
    """Failed to initiate a clean shutdown sequence."""


class ServerRunTestsError(ArangoServerError):
    """Failed to execute the specified tests on the server."""


class ServerTargetVersionError(ArangoServerError):
    """Failed to retrieve the target version."""


class ServerReadLogError(ArangoServerError):
    """Failed to retrieve the global log."""


class ServerLogLevelError(ArangoServerError):
    """Failed to retrieve the log level."""


class ServerLogLevelSetError(ArangoServerError):
    """Failed to set the log level."""


class ServerReloadRoutingError(ArangoServerError):
    """Failed to reload the routing information."""


class ServerStatisticsError(ArangoServerError):
    """Failed to retrieve the server statistics."""


class ServerRoleError(ArangoServerError):
    """Failed to retrieve the server role."""


#####################
# Task Exceptions   #
#####################


class TaskListError(ArangoServerError):
    """Failed to list the active server tasks."""


class TaskGetError(ArangoServerError):
    """Failed to retrieve the active server task."""


class TaskCreateError(ArangoServerError):
    """Failed to create the server task."""


class TaskDeleteError(ArangoServerError):
    """Failed to delete the server task."""


##########################
# Transaction Exceptions #
##########################


class TransactionStateError(ArangoClientError):
    """The transaction was in bad state and not executable."""


class TransactionJobResultError(ArangoClientError):
    """Failed to retrieve transaction job result."""


class TransactionExecuteError(ArangoServerError):
    """Failed to execute the transaction."""


###################
# User Exceptions #
###################


class UserListError(ArangoServerError):
    """Failed to retrieve the users."""


class UserGetError(ArangoServerError):
    """Failed to retrieve the user."""


class UserCreateError(ArangoServerError):
    """Failed to create the user."""


class UserUpdateError(ArangoServerError):
    """Failed to update the user."""


class UserReplaceError(ArangoServerError):
    """Failed to replace the user."""


class UserDeleteError(ArangoServerError):
    """Failed to delete the user."""


#########################
# Permission Exceptions #
#########################


class PermissionGetError(ArangoServerError):
    """Failed to retrieve the user permission."""


class PermissionUpdateError(ArangoServerError):
    """Failed to update the user permission."""


class PermissionClearError(ArangoServerError):
    """Failed to delete the user permission."""


##################
# WAL Exceptions #
##################


class WALPropertiesError(ArangoServerError):
    """Failed to retrieve the write-ahead log."""


class WALConfigureError(ArangoServerError):
    """Failed to configure the write-ahead log."""


class WALTransactionListError(ArangoServerError):
    """Failed to retrieve the list of running transactions."""


class WALFlushError(ArangoServerError):
    """Failed to flush the write-ahead log."""
