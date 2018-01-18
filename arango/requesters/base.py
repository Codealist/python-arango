from __future__ import absolute_import, unicode_literals

import logging

from arango.http_clients import DefaultHTTPClient
from arango.utils import sanitize_data, sanitize_params
from arango.aql import AQL
from arango.graph import Graph
from arango.collections import Collection


class Requester(object):
    """ArangoDB API requester.

    :param protocol: The internet transfer protocol (default: "http").
    :type protocol: str | unicode
    :param host: ArangoDB host (default: "localhost").
    :type host: str | unicode
    :param port: ArangoDB port (default: 8529).
    :type port: int | str | unicode
    :param database: The name of the target database (default: "_system").
    :type database: str | unicode
    :param username: ArangoDB username (default: "root").
    :type username: str | unicode
    :param password: ArangoDB password (default: "").
    :type password: str | unicode
    :param http_client: The HTTP client.
    :type http_client: arango.clients.base.BaseHTTPClient
    :param logger: The logger to record all API requests with.
    :type logger: logging.Logger
    """

    def __init__(self,
                 protocol='http',
                 host='localhost',
                 port=8529,
                 database='_system',
                 username='root',
                 password='',
                 http_client=None,
                 logger=None):
        self._type = 'standard'
        self._protocol = protocol.strip('/')
        self._host = host.strip('/')
        self._port = port
        self._database = database
        self._url_prefix = '{}://{}:{}/_db/{}'.format(
            self._protocol,
            self._host,
            self._port,
            self._database
        )
        self._username = username
        self._password = password
        self._http_client = http_client or DefaultHTTPClient()
        self._logger = logger or logging.getLogger('arango')
        self._aql = AQL(self)

    def __repr__(self):
        return '<ArangoDB connection to database "{}">'.format(self._database)

    @property
    def protocol(self):
        """Return the internet transfer protocol.

        :return: The internet transfer protocol
        :rtype: str | unicode
        """
        return self._protocol

    @property
    def host(self):
        """Return the ArangoDB host.

        :return: The ArangoDB host
        :rtype: str | unicode
        """
        return self._host

    @property
    def port(self):
        """Return the ArangoDB port.

        :return: The ArangoDB port
        :rtype: int
        """
        return self._port

    @property
    def username(self):
        """Return the ArangoDB username.

        :return: The ArangoDB username
        :rtype: str | unicode
        """
        return self._username

    @property
    def password(self):
        """Return the ArangoDB user password.

        :return: The ArangoDB user password
        :rtype: str | unicode
        """
        return self._password

    @property
    def database(self):
        """Return the name of the connected database.

        :return: The name of the connected database
        :rtype: str | unicode
        """
        return self._database

    @property
    def http_client(self):
        """Return the HTTP client in use.

        :return: The HTTP client in use
        :rtype: arango.http_clients.base.BaseHTTPClient
        """
        return self._http_client

    @property
    def type(self):
        """Return the connection type.

        :return: The connection type
        :rtype: str | unicode
        """
        return self._type

    @property
    def logger(self):
        """Return the logger.

        :return: The logger.
        :rtype: str | unicode
        """
        return self._logger

    def _execute_request(self, request, response_handler=None):
        """Make an API request synchronously.

        :param request: The request to make.
        :type request: arango.request.Request
        :param response_handler: The response handler function. If set to None,
            return the response immediately.
        :type response_handler: callable
        :return: The output of the response handler.
        :rtype: requests.Response | object
        """
        request.url = self._url_prefix + request.url
        request.data = sanitize_data(request.data)
        request.params = sanitize_params(request.params)
        if request.auth is None:
            request.auth = (self._username, self._password)
        self._logger.debug('{} {}'.format(request.method, request.url))

        response = self._http_client.make_request(request)
        return response_handler(response) if response_handler else response

    def execute_request(self, request, response_handler):
        """Make an API request synchronously.

        :param request: The request to make.
        :type request: arango.request.Request
        :param response_handler: The response handler function.
        :type response_handler: callable
        :return: The output of the response handler.
        :rtype: object
        """
        return self._execute_request(request, response_handler)

    @property
    def aql(self):
        """Return the AQL object tailored for asynchronous execution.

        API requests via the returned query object are placed in a server-side
        in-memory task queue and executed asynchronously in a fire-and-forget
        style.

        :return: ArangoDB query object
        :rtype: arango.query.AQL
        """
        return self._aql

    def collection(self, name):
        """Return a collection object tailored for asynchronous execution.

        API requests via the returned collection object are placed in a
        server-side in-memory task queue and executed asynchronously in
        a fire-and-forget style.

        :param name: The name of the collection
        :type name: str | unicode
        :return: The collection object
        :rtype: arango.collections.Collection
        """
        return Collection(self, name)

    def graph(self, name):
        """Return a graph object tailored for asynchronous execution.

        API requests via the returned graph object are placed in a server-side
        in-memory task queue and executed asynchronously in a fire-and-forget
        style.

        :param name: The name of the graph
        :type name: str | unicode
        :return: The graph object
        :rtype: arango.graph.Graph
        """
        return Graph(self, name)
