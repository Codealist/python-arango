from arango.exceptions import ArangoError


class WALPropertiesError(ArangoError):
    """Failed to retrieve the write-ahead log."""


class WALConfigureError(ArangoError):
    """Failed to configure the write-ahead log."""


class WALTransactionListError(ArangoError):
    """Failed to retrieve the list of running transactions."""


class WALFlushError(ArangoError):
    """Failed to flush the write-ahead log."""
