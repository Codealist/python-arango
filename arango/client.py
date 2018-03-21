from __future__ import absolute_import, unicode_literals

__all__ = ['ArangoClient']

import requests

from arango.exceptions import ServerConnectionError
from arango.api import APIExecutor
from arango.connection import Connection
from arango.database import Database
from arango.version import __version__


class ArangoClient(object):
    """ArangoDB client.

    :param protocol: Internet transfer protocol (default: "http").
    :type protocol: str or unicode
    :param host: ArangoDB host (default: "127.0.0.1").
    :type host: str or unicode
    :param port: ArangoDB port (default: 8529).
    :type port: int
    :param session: Custom requests session object. If not provided, session
        with default settings (i.e. requests.Session()) is used.
    :type session: requests.Session
    :param request_kwargs: Additional keyword arguments passed into the
        session object when sending an HTTP request.
    :type request_kwargs: dict.
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
        :rtype: str or unicode
        """
        return __version__

    @property
    def protocol(self):
        """Return the internet transfer protocol.

        :return: Internet transfer protocol.
        :rtype: str or unicode
        """
        return self._protocol

    @property
    def host(self):
        """Return the ArangoDB host.

        :return: ArangoDB host.
        :rtype: str or unicode
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
    def session(self):
        """Return the requests session.

        :return: Requests session.
        :rtype: requests.Session or None
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
        :type name: str or unicode
        :param username: Username for basic authentication.
        :type username: str or unicode
        :param password: Password for basic authentication.
        :type password: str or unicode
        :param verify: Verify the connection on initialization.
        :type verify: bool
        :return: Database wrapper.
        :rtype: arango.database.Database
        """
        connection = Connection(
            url=self._url,
            db=name,
            username=username,
            password=password,
            session=self._session,
            request_kwargs=self._request_kwargs
        )
        executor = APIExecutor()
        database = Database(connection, executor)

        if verify:
            try:
                database.ping()
            except ServerConnectionError as err:
                raise err
            except Exception as err:
                raise ServerConnectionError(
                    message='bad connection: {}'.format(err))

        return database
