from arango.exceptions.base import ArangoError


class AQLQueryExplainError(ArangoError):
    """Failed to explain the AQL query."""


class AQLQueryValidateError(ArangoError):
    """Failed to validate the AQL query."""


class AQLQueryExecuteError(ArangoError):
    """Failed to execute the AQL query."""


class AQLCacheClearError(ArangoError):
    """Failed to clear the AQL query cache."""


class AQLCachePropertiesError(ArangoError):
    """Failed to retrieve the AQL query cache properties."""


class AQLCacheConfigureError(ArangoError):
    """Failed to configure the AQL query cache properties."""


class AQLFunctionListError(ArangoError):
    """Failed to retrieve the list of AQL user functions."""


class AQLFunctionCreateError(ArangoError):
    """Failed to create the AQL user function."""


class AQLFunctionDeleteError(ArangoError):
    """Failed to delete the AQL user function."""
