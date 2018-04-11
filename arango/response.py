from __future__ import absolute_import, unicode_literals

__all__ = ['Response']

import json


class Response(object):
    """Synchronous ArangoDB HTTP response.

    Returned by :class:`arango.http.connection.BlockingHTTPConnection`.

    :param method: HTTP method in lowercase.
    :type method: str | unicode
    :param url: Request URL.
    :type url: str | unicode
    :param headers: Response headers.
    :type headers: dict
    :param status_code: HTTP status code.
    :type status_code: int
    :param status_text: HTTP status text (reason).
    :type status_text: str | unicode
    :param raw_body: Raw response body.
    :type raw_body: str | unicode
    """

    __slots__ = (
        '_method',
        '_url',
        '_headers',
        '_status_code',
        '_status_text',
        '_body',
        '_raw_body',
        '_error_code',
        '_error_message',
        '_is_success',
    )

    def __init__(self,
                 method,
                 url,
                 headers,
                 status_code,
                 status_text,
                 raw_body):
        self._method = method
        self._url = url
        self._headers = headers
        self._status_code = status_code
        self._status_text = status_text
        self._raw_body = raw_body
        try:
            self._body = json.loads(raw_body)
        except (ValueError, TypeError):
            self._body = raw_body
        if isinstance(self._body, dict):
            self._error_code = self._body.get('errorNum')
            self._error_message = self._body.get('errorMessage')
        else:
            self._error_code = None
            self._error_message = None

        http_ok = 200 <= status_code < 300
        self._is_success = http_ok and self._error_code is None

    @property
    def method(self):
        """Return the HTTP method in lowercase.

        :return: HTTP method (e.g. "post").
        :rtype: str | unicode
        """
        return self._method

    @property
    def url(self):
        """Return the request URL.

        :return: Request URL.
        :rtype: str | unicode
        """
        return self._url

    @property
    def headers(self):
        """Return the response headers.

        :return: Response headers.
        :rtype: dict
        """
        return self._headers

    @property
    def status_code(self):
        """Return the HTTP status code.

        :return: Response headers.
        :rtype: int
        """
        return self._status_code

    @property
    def status_text(self):
        """Return the HTTP status text (reason).

        :return: Status text (reason).
        :rtype: str | unicode
        """
        return self._status_text

    @property
    def body(self):
        """Return the JSON response body.

         :return: JSON response body.
         :rtype: object
         """
        return self._body

    @property
    def raw_body(self):
        """Return the raw response body.

        :return: Raw response body.
        :rtype: str | unicode
        """
        return self._raw_body

    @property
    def error_code(self):
        """Return the ArangoDB error code.

        :return: ArangoDB error code.
        :rtype: str | unicode
        """
        return self._error_code

    @property
    def error_message(self):
        """Return the ArangoDB error message.

        :return: ArangoDB error message.
        :rtype: str | unicode
        """
        return self._error_message

    @property
    def is_success(self):
        """Return True if the request was successful.

        :return: True if successful.
        :rtype: bool
        """
        return self._is_success
