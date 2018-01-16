from arango.exceptions import ArangoError


class DatabaseListError(ArangoError):
    """Failed to retrieve the list of databases."""


class DatabasePropertiesError(ArangoError):
    """Failed to retrieve the database options."""


class DatabaseCreateError(ArangoError):
    """Failed to create the database."""


class DatabaseDeleteError(ArangoError):
    """Failed to delete the database."""
