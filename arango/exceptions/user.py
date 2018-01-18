from arango.exceptions import ArangoError


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


class UserAccessError(ArangoError):
    """Failed to retrieve the user access details."""


class UserGrantAccessError(ArangoError):
    """Failed to grant user access to a database."""


class UserRevokeAccessError(ArangoError):
    """Failed to revoke user access to a database."""
