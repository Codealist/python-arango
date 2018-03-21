from __future__ import absolute_import, unicode_literals

import json

from six import moves, string_types


class Request(object):
    """Abstraction of an API request.

    :param method: HTTP method in lowercase (e.g. "post").
    :type method: str or unicode
    :param endpoint: API URL endpoint.
    :type endpoint: str or unicode
    :param headers: Request headers.
    :type headers: dict
    :param params: URL parameters.
    :type params: dict
    :param data: Request payload.
    :type data: object
    :param command: Transaction command.
    :type command: str or unicode
    :param read: Collection read during a transaction.
    :type read: str or unicode
    :param write: Collection written to during a transaction.
    :type write: str or unicode
    """

    __slots__ = (
        'method',
        'endpoint',
        'headers',
        'params',
        'data',
        'command',
        'read',
        'write',
        'graph'
    )

    def __init__(self,
                 method,
                 endpoint,
                 headers=None,
                 params=None,
                 data=None,
                 command=None,
                 read=None,
                 write=None):
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        #self._params = params

        # Covert booleans in URL params to integers
        if params is not None:
            for key, val in params.items():
                if isinstance(val, bool):
                    params[key] = int(val)
        self.params = params

        # Normalize the payload
        if data is None:
            self.data = None
        elif isinstance(data, string_types):
            self.data = data
        else:
            self.data = json.dumps(data)

        self.command = command
        self.read = read
        self.write = write

    def __str__(self):
        """Return the request details in string form."""
        path = self.endpoint
        if self.params is not None:
            path += '?' + moves.urllib.parse.urlencode(self.params)
        request_strings = ['{} {} HTTP/1.1'.format(self.method, path)]
        if self.headers is not None:
            for key, value in self.headers.items():
                request_strings.append('{}: {}'.format(key, value))
        if self.data is not None:
            request_strings.append('\r\n{}'.format(self.data))
        return '\r\n'.join(request_strings)
