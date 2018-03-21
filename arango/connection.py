from __future__ import absolute_import, unicode_literals

import requests

from arango.response import Response


class Connection(object):
    """HTTP connection to a specific ArangoDB database.

    :param url: ArangoDB URL.
    :type url: str or unicode
    :param host: ArangoDB server host (default: "127.0.0.1").
    :type host: str or unicode
    :param port: ArangoDB server port (default: 8529).
    :type port: int
    :param db: ArangoDB database.
    :type db: str or unicode
    :param username: ArangoDB username.
    :type username: str or unicode
    :param password: ArangoDB password.
    :type password: str or unicode
    :param session: Custom requests session object. If not provided, session
        with default settings (i.e. requests.Session()) is used.
    :type session: requests.Session
    :param request_kwargs: Additional keyword arguments passed into the
        session object when sending an HTTP request.
    :type request_kwargs: dict.
    """

    def __init__(self, url, db, username, password, session, request_kwargs):
        self._url_prefix = '{}/_db/{}'.format(url, db)
        self._database = db
        self._username = username
        self._auth = (username, password)
        self._session = session
        self._request_kwargs = request_kwargs

    @property
    def url_prefix(self):
        """Return the ArangoDB URL prefix.

        :returns: ArangoDB URL prefix.
        :rtype: str or unicode
        """
        return self._url_prefix

    @property
    def username(self):
        """Return the ArangoDB username.

        :returns: ArangoDB username.
        :rtype: str or unicode
        """
        return self._username

    @property
    def database(self):
        """Return the ArangoDB database.

        :returns: ArangoDB database.
        :rtype: str or unicode
        """
        return self._database

    def send_request(self, request):
        """Send an HTTP request to ArangoDB server.

        :param request: HTTP request.
        :type request: arango.request.Request
        :return: HTTP response.
        :rtype: arango.response.BaseResponse
        """
        request.headers['content-type'] = 'application/json; charset=utf-8'
        request.headers['x-content-type-options'] = 'nosniff'

        response = self._session.request(
            method=request.method,
            url=self._url_prefix + request.endpoint,
            params=request.params,
            data=request.data,
            headers=request.headers,
            auth=self._auth,
            **self._request_kwargs
        )
        return Response(
            method=response.request.method,
            url=response.url,
            headers=response.headers,
            status_code=response.status_code,
            status_text=response.reason,
            raw_body=response.text,
        )
