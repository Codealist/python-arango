from __future__ import absolute_import, unicode_literals

__all__ = ['ArangoClient']

import requests

from arango.connection import Connection
from arango.database import DefaultDatabase
from arango.exceptions import ServerConnectionError
from arango.version import __version__


class ArangoClient(object):
    """ArangoDB client.

    :param protocol: Internet transfer protocol (default: "http").
    :type protocol: str | unicode
    :param host: ArangoDB host (default: "127.0.0.1").
    :type host: str | unicode
    :param port: ArangoDB port (default: 8529).
    :type port: int
    :param session: User-defined requests_ session object. If not provided,
        session with default settings is used (i.e. requests.Session()).
    :type session: requests.Session
    :param request_kwargs: Additional keyword arguments passed into the
        requests_ session object when sending an HTTP request to ArangoDB.
    :type request_kwargs: dict.

    .. _requests: https://github.com/requests/requests
    """

    def __init__(self,
                 protocol='http',
                 host='127.0.0.1',
                 port=8529,
                 session=None,
                 request_kwargs=None):
        self._protocol = protocol.strip('/')
        self._host = host.strip('/')
        self._port = int(port)
        self._url = '{}://{}:{}'.format(protocol, host, port)
        self._session = session or requests.Session()
        self._request_kwargs = request_kwargs or {}

    def __repr__(self):
        return '<ArangoClient {}>'.format(self._url)

    @property
    def version(self):
        """Return the client version.

        :return: Client version.
        :rtype: str | unicode
        """
        return __version__

    @property
    def protocol(self):
        """Return the internet transfer protocol (e.g. "http").

        :return: Internet transfer protocol.
        :rtype: str | unicode
        """
        return self._protocol

    @property
    def host(self):
        """Return the ArangoDB host.

        :return: ArangoDB host.
        :rtype: str | unicode
        """
        return self._host

    @property
    def port(self):
        """Return the ArangoDB port.

        :return: ArangoDB port.
        :rtype: int
        """
        return self._port

    @property
    def url(self):
        """Return the ArangoDB URL.

        :return: ArangoDB URL.
        :rtype: str | unicode
        """
        return self._url

    @property
    def session(self):
        """Return the requests session.

        :return: Requests session.
        :rtype: requests.Session
        """
        return self._session

    @property
    def request_kwargs(self):
        """Return the request keyword arguments.

        :return: Request keyword arguments.
        :rtype: dict
        """
        return self._request_kwargs

    def db(self, name='_system', username='root', password='', verify=True):
        """Connect to a database and return the database API wrapper.

        :param name: Database name.
        :type name: str | unicode
        :param username: Username for basic authentication.
        :type username: str | unicode
        :param password: Password for basic authentication.
        :type password: str | unicode
        :param verify: Verify the connection on initialization of the wrapper.
        :type verify: bool
        :return: Database wrapper.
        :rtype: arango.database.DefaultDatabase
        """
        connection = Connection(
            url=self._url,
            db=name,
            username=username,
            password=password,
            session=self._session,
            request_kwargs=self._request_kwargs
        )
        database = DefaultDatabase(connection)

        if verify:  # Check the server connection by making a read API call
            try:
                database.ping()
            except ServerConnectionError as err:
                raise err
            except Exception as err:
                raise ServerConnectionError('bad connection: {}'.format(err))

        return database
