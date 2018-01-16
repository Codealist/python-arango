from arango.exceptions import ArangoError


class TaskListError(ArangoError):
    """Failed to list the active server tasks."""


class TaskGetError(ArangoError):
    """Failed to retrieve the active server task."""


class TaskCreateError(ArangoError):
    """Failed to create a server task."""


class TaskDeleteError(ArangoError):
    """Failed to delete a server task."""
