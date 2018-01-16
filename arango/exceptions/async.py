from arango.exceptions import ArangoError


class AsyncExecuteError(ArangoError):
    """Failed to execute the asynchronous request."""


class AsyncJobListError(ArangoError):
    """Failed to list the IDs of the asynchronous jobs."""


class AsyncJobCancelError(ArangoError):
    """Failed to cancel the asynchronous job."""


class AsyncJobStatusError(ArangoError):
    """Failed to retrieve the asynchronous job result from the server."""


class AsyncJobResultError(ArangoError):
    """Failed to pop the asynchronous job result from the server."""


class AsyncJobClearError(ArangoError):
    """Failed to delete the asynchronous job result from the server."""
