import logging
from contextlib import contextmanager

from six import string_types

from arango.exceptions import DocumentParseError


@contextmanager
def suppress_warning(logger_name):
    """Suppress logger warning messages.

    :param logger_name: Full name of the logger.
    :type logger_name: str | unicode
    """
    logger = logging.getLogger(logger_name)
    original_log_level = logger.getEffectiveLevel()
    logger.setLevel(logging.CRITICAL)
    yield
    logger.setLevel(original_log_level)


def split_id(document):
    """Return the collection name and document key from ID.

    :param document: Document ID or body with "_id" field.
    :type document: str | unicode | dict
    :return: Collection name and document key.
    :rtype: [str | unicode, str | unicode]
    """
    try:
        doc_id = document['_id'] if isinstance(document, dict) else document
    except KeyError:
        raise DocumentParseError('field "_id" required')
    return doc_id.split('/', 1)
