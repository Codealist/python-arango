from arango.exceptions import ArangoError


class PregelJobCreateError(ArangoError):
    """Failed to start/create a Pregel job."""


class PregelJobGetError(ArangoError):
    """Failed to retrieve a Pregel job."""


class PregelJobDeleteError(ArangoError):
    """Failed to cancel/delete a Pregel job."""
