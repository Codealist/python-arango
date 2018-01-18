from arango import Request
from arango.requesters import Requester
from arango.database import Database
from arango.exceptions import (
    DatabaseCreateError,
    DatabaseDeleteError,
    ServerEndpointsError
)
from arango.http_clients import DefaultHTTPClient
from arango.utils import HTTP_OK


class ArangoClient(Database):
    """ArangoDB Client.

    :param protocol: Internet transfer protocol (default: "http").
    :type protocol: str | unicode
    :param host: ArangoDB server host (default: "localhost").
    :type host: str | unicode
    :param port: ArangoDB server port (default: 8529).
    :type port: int or str
    :param username: ArangoDB default username (default: "root") used for
        basic HTTP authentication.
    :type username: str | unicode
    :param password: ArangoDB default password (default: "") used for basic
        HTTP authentication.
    :param verify: If set to True, a sample API call is sent to verify the
        connection during initialization of this client. Root privileges are
        required to use this parameter.
    :type verify: bool
    :param http_client: Custom HTTP client to override the default one with.
    :type http_client: arango.http_clients.base.BaseHTTPClient
    :param check_cert: Verify SSL certificate when making HTTP requests.
        This flag is ignored if a custom **http_client** is specified.
    :type check_cert: bool
    :param use_session: Use session when making HTTP requests. This flag is
        ignored if a custom **http_client** is specified.
    :type use_session: bool
    :param logger: Custom logger to record the API requests with. The
        logger's ``debug`` method is called.
    :type logger: logging.Logger
    """

    def __init__(self,
                 protocol='http',
                 host='127.0.0.1',
                 port=8529,
                 username='root',
                 password='',
                 verify=False,
                 http_client=None,
                 check_cert=True,
                 use_session=True,
                 logger=None):

        if http_client is None:
            http_client = DefaultHTTPClient(
                use_session=use_session,
                check_cert=check_cert
            )
        requester = Requester(
            protocol=protocol,
            host=host,
            port=port,
            database='_system',
            username=username,
            password=password,
            http_client=http_client,
            logger=logger
        )
        super(ArangoClient, self).__init__(requester)

        if verify:
            self.verify()

    def __repr__(self):
        return '<ArangoDB client for "{}">'.format(self.requester.host)

    def __getitem__(self, name):
        """Return the database object.

        :param name: The name of the database.
        :type name: str | unicode
        :return: The database object.
        :rtype: arango.database.Database
        """
        return self.database(name)

    def endpoints(self):
        """Return the list of the endpoints the server is listening on.

        Each endpoint is mapped to a list of databases. If the list is empty,
        it means all databases can be accessed via the endpoint. If the list
        contains more than one database, the first database receives all the
        requests by default unless the database name is explicitly specified.

        :return: The list of endpoints.
        :rtype: list
        :raise arango.exceptions.ServerEndpointsError: If the retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/endpoint'
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise ServerEndpointsError(res)
            return res.body

        return self._execute_request(request, handler)

    def db(self, name, username=None, password=None):
        """Return the database object.

        :param name: The name of the database.
        :type name: str | unicode
        :param username: The username for basic authentication. Overrides the
            username specified during client initialization.
        :type username: str | unicode
        :param password: The password for basic authentication. Overrides the
            password specified during the client initialization.
        :type password: str | unicode
        :return: The database object.
        :rtype: arango.database.Database

        .. note::
            This is an alias for :func:`arango.database.Database.database`.
        """
        return self.database(name, username, password)

    def database(self, name, username=None, password=None):
        """Return the database object.

        :param name: The name of the database
        :type name: str | unicode
        :param username: The username for authentication (if set, overrides
            the username specified during the client initialization)
        :type username: str | unicode
        :param password: The password for authentication (if set, overrides
            the password specified during the client initialization
        :type password: str | unicode
        :return: The database object
        :rtype: arango.database.Database
        """
        if username is None:
            username = self.username

        if password is None:
            password = self.password

        return Database(Requester(
            protocol=self.protocol,
            host=self.host,
            port=self.port,
            database=name,
            username=username,
            password=password,
            http_client=self.http_client,
        ))

    def create_database(self, name, users=None, username=None, password=None):
        """Create a new database.

        :param name: The name of the new database
        :type name: str | unicode
        :param users: The list of users with access to the new database, where
            each user is a dictionary with keys "username", "password",
            "active" and "extra".
        :type users: [dict]
        :param username: The username for authentication (if set, overrides
            the username specified during the client initialization)
        :type username: str | unicode
        :param password: The password for authentication (if set, overrides
            the password specified during the client initialization
        :type password: str | unicode
        :return: The database object
        :rtype: arango.database.Database
        :raise arango.exceptions.DatabaseCreateError: If the create fails

        .. note::
            Here is an example entry in **users**:

            .. code-block:: python

                {
                    'username': 'john',
                    'password': 'password',
                    'active': True,
                    'extra': {'Department': 'IT'}
                }

            If **users** is not set, only the root and the current user are
            granted access to the new database by default.
        """

        data = {
            'name': name,
        }

        if users is not None:
            data['users'] = [{
                'username': user['username'],
                'passwd': user['password'],
                'active': user.get('active', True),
                'extra': user.get('extra', {})
            } for user in users]

        request = Request(
            method='post',
            endpoint='/_api/database',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DatabaseCreateError(res)
            return self.db(name, username, password)

        return self._execute_request(request, handler)

    def delete_database(self, name, ignore_missing=False):
        """Delete the database of the specified name.

        :param name: The name of the database to delete
        :type name: str | unicode
        :param ignore_missing: ignore missing databases
        :type ignore_missing: bool
        :return: whether the database was deleted successfully
        :rtype: bool
        :raise arango.exceptions.DatabaseDeleteError: If request fails

        .. note::
            Root privileges (i.e. access to the ``_system`` database) are
            required to use this method.
        """

        request = Request(
            method='delete',
            endpoint='/_api/database/{}'.format(name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                if not (res.status_code == 404 and ignore_missing):
                    raise DatabaseDeleteError(res)
            return not res.body['error']

        return self._execute_request(request, handler)
