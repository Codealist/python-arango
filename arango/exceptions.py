from __future__ import absolute_import, unicode_literals


class ArangoError(Exception):
    """ArangoDB API request exception.

    :param response: HTTP response.
    :type response: arango.http.responses.abc.HTTPResponse
    :param message: Optional error message (overrides the default).
    :type message: str or unicode

    :ivar message: Error message.
    :var_type message: str or unicode
    :ivar http_method: HTTP method (e.g. "post").
    :var_type http_method: str or unicode
    :ivar url: Request URL.
    :var_type url: str or unicode
    :ivar http_code: HTTP status code.
    :var_type http_code: int
    :ivar http_headers: Response headers.
    :var_type http_headers: dict
    """

    def __init__(self, response=None, message=None):
        if response is None:
            super(ArangoError, self).__init__(message)
            self.message = message
            self.http_method = None
            self.url = None
            self.http_code = None
            self.http_headers = None
        else:
            if message is not None:
                message = message
            elif response.error_message is not None:
                message = response.error_message
            else:
                message = response.status_text

            self.error_code = response.error_code
            if self.error_code is None:
                message = '[HTTP {}] {}'.format(
                    response.status_code,
                    message
                )
            else:
                message = '[HTTP {}][ERR {}] {}'.format(
                    response.status_code,
                    self.error_code,
                    message
                )
            # Generate the error message for the exception
            super(ArangoError, self).__init__(message)
            self.message = message
            self.http_method = response.method
            self.url = response.url
            self.http_code = response.status_code
            self.http_headers = response.headers

##################
# API Exceptions #
##################


class APIContextError(ArangoError):
    """Failed to execute API call under given context."""


##################
# AQL Exceptions #
##################


class AQLQueryExplainError(ArangoError):
    """Failed to explain an AQL query."""


class AQLQueryValidateError(ArangoError):
    """Failed to validate an AQL query."""


class AQLQueryExecuteError(ArangoError):
    """Failed to execute an AQL query."""


class AQLRunningQueryGetError(ArangoError):
    """Failed to retrieve running AQL queries."""


class AQLSlowQueryGetError(ArangoError):
    """Failed to retrieve slow AQL queries."""


class AQLSlowQueryClearError(ArangoError):
    """Failed to clear slow AQL queries."""


class AQLQueryTrackingGetError(ArangoError):
    """Failed to retrieve AQL tracking properties."""


class AQLQueryTrackingSetError(ArangoError):
    """Failed to configure AQL tracking properties."""


class AQLQueryKillError(ArangoError):
    """Failed to kill an AQL query."""


class AQLCacheClearError(ArangoError):
    """Failed to clear the AQL query cache."""


class AQLCachePropertiesError(ArangoError):
    """Failed to retrieve the AQL query cache properties."""


class AQLCacheConfigureError(ArangoError):
    """Failed to configure the AQL query cache properties."""


class AQLFunctionListError(ArangoError):
    """Failed to list AQL user functions."""


class AQLFunctionCreateError(ArangoError):
    """Failed to create an AQL user function."""


class AQLFunctionDeleteError(ArangoError):
    """Failed to delete an AQL user function."""


####################
# Async Exceptions #
####################


class AsyncExecuteError(ArangoError):
    """Failed to execute asynchronous API."""


class AsyncJobListError(ArangoError):
    """Failed to list asynchronous jobs."""


class AsyncJobCancelError(ArangoError):
    """Failed to cancel asynchronous job."""


class AsyncJobStatusError(ArangoError):
    """Failed to retrieve asynchronous job status."""


class AsyncJobResultError(ArangoError):
    """Failed to retrieve asynchronous job result."""


class AsyncJobClearError(ArangoError):
    """Failed to delete asynchronous job result."""


####################
# Batch Exceptions #
####################


class BatchBadStateError(ArangoError):
    """The batch was in bad state and not executable."""


class BatchExecuteError(ArangoError):
    """Failed to execute batch API."""


class BatchJobResultError(ArangoError):
    """Failed to retrieve batch job result."""


#########################
# Collection Exceptions #
#########################


class CollectionListError(ArangoError):
    """Failed to list collections."""


class CollectionPropertiesError(ArangoError):
    """Failed to retrieve collection properties."""


class CollectionConfigureError(ArangoError):
    """Failed to configure collection properties."""


class CollectionStatisticsError(ArangoError):
    """Failed to retrieve collection statistics."""


class CollectionRevisionError(ArangoError):
    """Failed to retrieve collection revision."""


class CollectionChecksumError(ArangoError):
    """Failed to retrieve collection checksum."""


class CollectionCreateError(ArangoError):
    """Failed to create collection."""


class CollectionDeleteError(ArangoError):
    """Failed to delete collection."""


class CollectionRenameError(ArangoError):
    """Failed to rename collection."""


class CollectionTruncateError(ArangoError):
    """Failed to truncate collection."""


class CollectionLoadError(ArangoError):
    """Failed to load collection in memory."""


class CollectionUnloadError(ArangoError):
    """Failed to unload collection in memory."""


class CollectionRotateJournalError(ArangoError):
    """Failed to rotate the journal of the collection."""


#####################
# Cursor Exceptions #
#####################


class CursorNextError(ArangoError):
    """Failed to retrieve the next cursor result."""


class CursorCloseError(ArangoError):
    """Failed to delete the cursor from the server."""


#######################
# Database Exceptions #
#######################


class DatabaseListError(ArangoError):
    """Failed to retrieve the list of databases."""


class DatabasePropertiesError(ArangoError):
    """Failed to retrieve the database options."""


class DatabaseCreateError(ArangoError):
    """Failed to create the database."""


class DatabaseDeleteError(ArangoError):
    """Failed to delete the database."""


#######################
# Document Exceptions #
#######################


class DocumentCountError(ArangoError):
    """Failed to retrieve the count of the documents in the collections."""


class DocumentInError(ArangoError):
    """Failed to check whether a collection contains a document."""


class DocumentGetError(ArangoError):
    """Failed to retrieve the document."""


class DocumentKeysError(ArangoError):
    """Failed to retrieve the document keys."""


class DocumentIDsError(ArangoError):
    """Failed to retrieve the document IDs."""


class DocumentParseError(ArangoError):
    """Failed to retrieve the document key."""


class DocumentInsertError(ArangoError):
    """Failed to insert the document."""


class DocumentReplaceError(ArangoError):
    """Failed to replace the document."""


class DocumentUpdateError(ArangoError):
    """Failed to update the document."""


class DocumentDeleteError(ArangoError):
    """Failed to delete the document."""


class DocumentRevisionError(ArangoError):
    """The expected and actual document revisions do not match."""


###################
# Foxx Exceptions #
###################


class FoxxServiceListError(ArangoError):
    """Failed to list Foxx services."""


class FoxxServiceGetError(ArangoError):
    """Failed to retrieve Foxx service metadata."""


class FoxxServiceCreateError(ArangoError):
    """Failed to create a Foxx service."""


class FoxxServiceUpdateError(ArangoError):
    """Failed to update a Foxx service."""


class FoxxServiceReplaceError(ArangoError):
    """Failed to replace a Foxx service."""


class FoxxServiceDeleteError(ArangoError):
    """Failed to delete a Foxx services."""


class FoxxConfigGetError(ArangoError):
    """Failed to retrieve Foxx service configuration."""


class FoxxConfigUpdateError(ArangoError):
    """Failed to update Foxx service configuration."""


class FoxxConfigReplaceError(ArangoError):
    """Failed to replace Foxx service configuration."""


class FoxxDependencyGetError(ArangoError):
    """Failed to retrieve Foxx service dependencies."""


class FoxxDependencyUpdateError(ArangoError):
    """Failed to update Foxx service dependencies."""


class FoxxDependencyReplaceError(ArangoError):
    """Failed to replace Foxx service dependencies."""


class FoxxScriptListError(ArangoError):
    """Failed to list Foxx service scripts."""


class FoxxScriptRunError(ArangoError):
    """Failed to run Foxx service script."""


class FoxxTestRunError(ArangoError):
    """Failed to run Foxx service tests."""


class FoxxDevEnableError(ArangoError):
    """Failed to enable development mode for a service."""


class FoxxDevDisableError(ArangoError):
    """Failed to disable development mode for a service."""


class FoxxReadmeGetError(ArangoError):
    """Failed to retrieve service readme."""


class FoxxSwaggerGetError(ArangoError):
    """Failed to retrieve swagger description."""


class FoxxDownloadError(ArangoError):
    """Failed to download service bundle."""


class FoxxCommitError(ArangoError):
    """Failed to commit local service state."""


####################
# Graph Exceptions #
####################


class GraphListError(ArangoError):
    """Failed to retrieve the list of graphs."""


class GraphGetError(ArangoError):
    """Failed to retrieve the graph."""


class GraphCreateError(ArangoError):
    """Failed to create the graph."""


class GraphDeleteError(ArangoError):
    """Failed to delete the graph."""


class GraphPropertiesError(ArangoError):
    """Failed to retrieve the graph properties."""


class GraphTraverseError(ArangoError):
    """Failed to execute the graph traversal."""


class OrphanCollectionListError(ArangoError):
    """Failed to retrieve the list of orphan vertex collections."""


class VertexCollectionListError(ArangoError):
    """Failed to retrieve the list of vertex collections."""


class VertexCollectionCreateError(ArangoError):
    """Failed to create the vertex collection."""


class VertexCollectionDeleteError(ArangoError):
    """Failed to delete the vertex collection."""


class EdgeDefinitionListError(ArangoError):
    """Failed to retrieve the list of edge definitions."""


class EdgeDefinitionCreateError(ArangoError):
    """Failed to create the edge definition."""


class EdgeDefinitionReplaceError(ArangoError):
    """Failed to replace the edge definition."""


class EdgeDefinitionDeleteError(ArangoError):
    """Failed to delete the edge definition."""


class EdgeListError(ArangoError):
    """Failed to retrieve edges coming in and out of a vertex."""


####################
# Index Exceptions #
####################


class IndexListError(ArangoError):
    """Failed to retrieve the list of indexes in the collection."""


class IndexCreateError(ArangoError):
    """Failed to create the index in the collection."""


class IndexDeleteError(ArangoError):
    """Failed to delete the index from the collection."""


#####################
# Pregel Exceptions #
#####################


class PregelJobCreateError(ArangoError):
    """Failed to create a Pregel job."""


class PregelJobGetError(ArangoError):
    """Failed to retrieve a Pregel job."""


class PregelJobDeleteError(ArangoError):
    """Failed to cancel a Pregel job."""


#####################
# Server Exceptions #
#####################


class ServerConnectionError(ArangoError):
    """Failed to connect to ArangoDB server."""


class ServerEngineError(ArangoError):
    """Failed to retrieve the database engine."""


class ServerEndpointsError(ArangoError):
    """Failed to retrieve the ArangoDB server endpoints."""


class ServerVersionError(ArangoError):
    """Failed to retrieve the ArangoDB server version."""


class ServerDetailsError(ArangoError):
    """Failed to retrieve the ArangoDB server details."""


class ServerTimeError(ArangoError):
    """Failed to return the current ArangoDB system time."""


class ServerEchoError(ArangoError):
    """Failed to return the last request."""


class ServerShutdownError(ArangoError):
    """Failed to initiate a clean shutdown sequence."""


class ServerRunTestsError(ArangoError):
    """Failed to execute the specified tests on the server."""


class ServerTargetVersionError(ArangoError):
    """Failed to retrieve the target version."""


class ServerReadLogError(ArangoError):
    """Failed to retrieve the global log."""


class ServerLogLevelError(ArangoError):
    """Failed to retrieve the log level."""


class ServerLogLevelSetError(ArangoError):
    """Failed to set the log level."""


class ServerReloadRoutingError(ArangoError):
    """Failed to reload the routing information."""


class ServerStatisticsError(ArangoError):
    """Failed to retrieve the server statistics."""


class ServerRoleError(ArangoError):
    """Failed to retrieve the server role."""


#####################
# Task Exceptions   #
#####################


class TaskListError(ArangoError):
    """Failed to list the active server tasks."""


class TaskGetError(ArangoError):
    """Failed to retrieve the active server task."""


class TaskCreateError(ArangoError):
    """Failed to create the server task."""


class TaskDeleteError(ArangoError):
    """Failed to delete the server task."""


##########################
# Transaction Exceptions #
##########################


class TransactionBadStateError(ArangoError):
    """The transaction was in bad state and not executable."""


class TransactionExecuteError(ArangoError):
    """Failed to execute the transaction."""


class TransactionJobResultError(ArangoError):
    """Failed to retrieve transaction job result."""


class TransactionJobQueueError(ArangoError):
    """Failed to queue a transaction job."""


###################
# User Exceptions #
###################


class UserListError(ArangoError):
    """Failed to retrieve the users."""


class UserGetError(ArangoError):
    """Failed to retrieve the user."""


class UserCreateError(ArangoError):
    """Failed to create the user."""


class UserUpdateError(ArangoError):
    """Failed to update the user."""


class UserReplaceError(ArangoError):
    """Failed to replace the user."""


class UserDeleteError(ArangoError):
    """Failed to delete the user."""


#########################
# Permission Exceptions #
#########################


class PermissionGetError(ArangoError):
    """Failed to retrieve the user permission."""


class PermissionUpdateError(ArangoError):
    """Failed to update the user permission."""


class PermissionDeleteError(ArangoError):
    """Failed to delete the user permission."""


##################
# WAL Exceptions #
##################


class WALPropertiesError(ArangoError):
    """Failed to retrieve the write-ahead log."""


class WALConfigureError(ArangoError):
    """Failed to configure the write-ahead log."""


class WALTransactionListError(ArangoError):
    """Failed to retrieve the list of running transactions."""


class WALFlushError(ArangoError):
    """Failed to flush the write-ahead log."""
