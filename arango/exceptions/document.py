from arango.exceptions import ArangoError


class DocumentCountError(ArangoError):
    """Failed to retrieve the count of the documents in the collections."""


class DocumentInError(ArangoError):
    """Failed to check whether a collection contains a document."""


class DocumentGetError(ArangoError):
    """Failed to retrieve the document."""


class DocumentInsertError(ArangoError):
    """Failed to insert the document."""


class DocumentReplaceError(ArangoError):
    """Failed to replace the document."""


class DocumentUpdateError(ArangoError):
    """Failed to update the document."""


class DocumentDeleteError(ArangoError):
    """Failed to delete the document."""


class DocumentRevisionError(ArangoError):
    """The expected and actual document revisions do not match."""
