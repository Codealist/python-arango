from arango.exceptions import ArangoError


class PregelJobCreateError(ArangoError):
    """Failed to create a Pregel job."""


class PregelJobGetError(ArangoError):
    """Failed to retrieve a Pregel job."""


class PregelJobDeleteError(ArangoError):
    """Failed to cancel a Pregel job."""
