from arango.exceptions import ArangoError


class CollectionListError(ArangoError):
    """Failed to retrieve the list of collections."""


class CollectionPropertiesError(ArangoError):
    """Failed to retrieve the collection properties."""


class CollectionConfigureError(ArangoError):
    """Failed to configure the collection properties."""


class CollectionStatisticsError(ArangoError):
    """Failed to retrieve the collection statistics."""


class CollectionRevisionError(ArangoError):
    """Failed to retrieve the collection revision."""


class CollectionChecksumError(ArangoError):
    """Failed to retrieve the collection checksum."""


class CollectionCreateError(ArangoError):
    """Failed to create the collection."""


class CollectionDeleteError(ArangoError):
    """Failed to delete the collection."""


class CollectionRenameError(ArangoError):
    """Failed to rename the collection."""


class CollectionTruncateError(ArangoError):
    """Failed to truncate the collection."""


class CollectionLoadError(ArangoError):
    """Failed to load the collection into memory."""


class CollectionUnloadError(ArangoError):
    """Failed to unload the collection from memory."""


class CollectionRotateJournalError(ArangoError):
    """Failed to rotate the journal of the collection."""


class CollectionBadStatusError(ArangoError):
    """Unknown status was returned from the collection."""
