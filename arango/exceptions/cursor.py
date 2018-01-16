from arango.exceptions import ArangoError


class CursorNextError(ArangoError):
    """Failed to retrieve the next cursor result."""


class CursorCloseError(ArangoError):
    """Failed to delete the cursor from the server."""
