import logging
from contextlib import contextmanager

from six import string_types


@contextmanager
def suppress_warning(logger_name):
    """Suppress logger warning messages.

    :param logger_name: Full name of the logger.
    :type logger_name: str or unicode
    """
    logger = logging.getLogger(logger_name)
    original_log_level = logger.getEffectiveLevel()
    logger.setLevel(logging.CRITICAL)
    yield
    logger.setLevel(original_log_level)


def is_dict(obj):
    return isinstance(obj, dict)

def is_list(obj):
    return isinstance(obj, list)

def is_bool(obj):
    return isinstance(obj, bool)

def is_int(obj):
    return isinstance(obj, int)

def is_str(obj):
    return isinstance(obj, string_types)
