from __future__ import absolute_import, unicode_literals

from json import dumps

from six import string_types


def sanitize_data(data):
    if data is None:
        return None
    elif isinstance(data, string_types):
        return data
    else:
        return dumps(data)


def sanitize_params(params):
    if params is None:
        return params

    sanitized_params = {}

    for param, value in params.items():
        if isinstance(value, bool):
            value = int(value)

        sanitized_params[param] = value

    return sanitized_params
