from arango.exceptions import ArangoError


class ServerConnectionError(ArangoError):
    """Failed to connect to the ArangoDB instance."""


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


class ServerSleepError(ArangoError):
    """Failed to suspend the ArangoDB server."""


class ServerShutdownError(ArangoError):
    """Failed to initiate a clean shutdown sequence."""


class ServerRunTestsError(ArangoError):
    """Failed to execute the specified tests on the server."""


class ServerExecuteError(ArangoError):
    """Failed to execute a the given Javascript program on the server."""


class ServerRequiredDBVersionError(ArangoError):
    """Failed to retrieve the required database version."""


class ServerReadLogError(ArangoError):
    """Failed to retrieve the global log."""


class ServerLogLevelError(ArangoError):
    """Failed to return the log level."""


class ServerLogLevelSetError(ArangoError):
    """Failed to set the log level."""


class ServerReloadRoutingError(ArangoError):
    """Failed to reload the routing information."""


class ServerStatisticsError(ArangoError):
    """Failed to retrieve the server statistics."""


class ServerRoleError(ArangoError):
    """Failed to retrieve the role of the server in a cluster."""
