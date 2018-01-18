from arango.exceptions import ArangoError


class GraphListError(ArangoError):
    """Failed to retrieve the list of graphs."""


class GraphGetError(ArangoError):
    """Failed to retrieve the graph."""


class GraphCreateError(ArangoError):
    """Failed to create the graph."""


class GraphDeleteError(ArangoError):
    """Failed to delete the graph."""


class GraphPropertiesError(ArangoError):
    """Failed to retrieve the graph properties."""


class GraphTraverseError(ArangoError):
    """Failed to execute the graph traversal."""


class OrphanCollectionListError(ArangoError):
    """Failed to retrieve the list of orphan vertex collections."""


class VertexCollectionListError(ArangoError):
    """Failed to retrieve the list of vertex collections."""


class VertexCollectionCreateError(ArangoError):
    """Failed to create the vertex collection."""


class VertexCollectionDeleteError(ArangoError):
    """Failed to delete the vertex collection."""


class EdgeDefinitionListError(ArangoError):
    """Failed to retrieve the list of edge definitions."""


class EdgeDefinitionCreateError(ArangoError):
    """Failed to create the edge definition."""


class EdgeDefinitionReplaceError(ArangoError):
    """Failed to replace the edge definition."""


class EdgeDefinitionDeleteError(ArangoError):
    """Failed to delete the edge definition."""
