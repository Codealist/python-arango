from arango.exceptions import ArangoError


class IndexListError(ArangoError):
    """Failed to retrieve the list of indexes in the collection."""


class IndexCreateError(ArangoError):
    """Failed to create the index in the collection."""


class IndexDeleteError(ArangoError):
    """Failed to delete the index from the collection."""
