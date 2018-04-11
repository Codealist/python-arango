from __future__ import absolute_import, unicode_literals

__all__ = ['Foxx']

from arango.api import APIWrapper
from arango.exceptions import (
    FoxxServiceCreateError,
    FoxxServiceDeleteError,
    FoxxServiceGetError,
    FoxxServiceListError,
    FoxxServiceReplaceError,
    FoxxServiceUpdateError,
    FoxxConfigGetError,
    FoxxConfigReplaceError,
    FoxxConfigUpdateError,
    FoxxDependencyGetError,
    FoxxDependencyReplaceError,
    FoxxDependencyUpdateError,
    FoxxScriptListError,
    FoxxScriptRunError,
    FoxxTestRunError,
    FoxxDevEnableError,
    FoxxDevDisableError,
    FoxxReadmeGetError,
    FoxxSwaggerGetError,
    FoxxCommitError,
    FoxxDownloadError,
)
from arango.request import Request


class Foxx(APIWrapper):
    """Wrapper for Foxx.

    :param connection: HTTP connection.
    :type connection: arango.connection.Connection
    :param executor: API executor.
    :type executor: arango.executor.DefaultExecutor
    """

    def __init__(self, connection, executor):
        super(Foxx, self).__init__(connection, executor)

    def __repr__(self):
        return '<Foxx in {}>'.format(self._conn.db_name)

    def services(self, exclude_system=False):
        """List installed services.

        :param exclude_system: Exclude system services.
        :type exclude_system: bool
        :return: List of system service.
        :rtype: [str | unicode | dict]
        :raise arango.exceptions.FoxxServiceListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx',
            params={'excludeSystem': exclude_system}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxServiceListError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def service(self, mount):
        """Return service metadata.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Service metadata.
        :rtype: dict
        :raise arango.exceptions.FoxxServiceGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx/service',
            params={'mount': mount}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxServiceGetError(resp)

            if 'manifest' in resp.body:
                manifest = resp.body['manifest']
                if 'defaultDocument' in manifest:
                    doc = manifest.pop('defaultDocument')
                    manifest['default_document'] = doc

            return resp.body

        return self._execute(request, response_handler)

    def create_service(self,
                       mount,
                       source,
                       config=None,
                       dependencies=None,
                       development=None,
                       setup=None,
                       legacy=None):
        """Install a new service.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param source: Fully qualified URL or absolute path on the server file
            system. Must be accessible by the server (or by all servers in a
            cluster).
        :type source: str | unicode
        :param config: Configuration values.
        :type config: dict
        :param dependencies: Dependency settings.
        :type dependencies: dict
        :param development: Enable development mode.
        :type development: bool
        :param setup: Run service setup script.
        :type setup: bool
        :param legacy: Install the service in 2.8 legacy compatibility mode.
        :type legacy: bool
        :return: The result of the installation.
        :rtype: dict
        :raise arango.exceptions.FoxxServiceCreateError: If install fails.
        """
        params = {'mount': mount}
        if development is not None:
            params['development'] = development
        if setup is not None:
            params['setup'] = setup
        if legacy is not None:
            params['legacy'] = legacy

        data = {'source': source}
        if config is not None:
            data['configuration'] = config
        if dependencies is not None:
            data['dependencies'] = dependencies

        request = Request(
            method='post',
            endpoint='/_api/foxx',
            params=params,
            data=data,
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxServiceCreateError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def update_service(self,
                       mount,
                       source=None,
                       config=None,
                       dependencies=None,
                       teardown=None,
                       setup=None,
                       legacy=None):
        """Update (upgrade) a service.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param source: Fully qualified URL or absolute path on the server file
            system. Must be accessible by the server (or by all servers in a
            cluster).
        :type source: str | unicode
        :param config: Configuration values.
        :type config: dict
        :param dependencies: Dependency settings.
        :type dependencies: dict
        :param teardown: Enable development mode.
        :type teardown: bool
        :param setup: Run service setup script.
        :type setup: bool
        :param legacy: Install the service in 2.8 legacy compatibility mode.
        :type legacy: bool
        :return: The result of the installation.
        :rtype: dict
        :raise arango.exceptions.FoxxServiceUpdateError: If update fails.
        """
        params = {'mount': mount}
        if teardown is not None:
            params['teardown'] = teardown
        if setup is not None:
            params['setup'] = setup
        if legacy is not None:
            params['legacy'] = legacy

        data = {'source': source}
        if config is not None:
            data['configuration'] = config
        if dependencies is not None:
            data['dependencies'] = dependencies

        request = Request(
            method='patch',
            endpoint='/_api/foxx/service',
            params=params,
            data=data,
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxServiceUpdateError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def replace_service(self,
                        mount,
                        source,
                        config=None,
                        dependencies=None,
                        teardown=None,
                        setup=None,
                        legacy=None,
                        force=None):
        """Replace a service by removing the old one and installing a new one.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param source: Fully qualified URL or absolute path on the server file
            system. Must be accessible by the server (or by all servers in a
            cluster).
        :type source: str | unicode
        :param config: Configuration values.
        :type config: dict
        :param dependencies: Dependency settings.
        :type dependencies: dict
        :param teardown: Enable development mode.
        :type teardown: bool
        :param setup: Run service setup script.
        :type setup: bool
        :param legacy: Install the service in 2.8 legacy compatibility mode.
        :type legacy: bool
        :param force: Force install if no service is found under given mount.
        :type force: bool
        :return: The result of the installation.
        :rtype: dict
        :raise arango.exceptions.FoxxServiceReplaceError: If replace fails.
        """
        params = {'mount': mount}
        if teardown is not None:
            params['teardown'] = teardown
        if setup is not None:
            params['setup'] = setup
        if legacy is not None:
            params['legacy'] = legacy
        if force is not None:
            params['force'] = force

        data = {'source': source}
        if config is not None:
            data['configuration'] = config
        if dependencies is not None:
            data['dependencies'] = dependencies

        request = Request(
            method='put',
            endpoint='/_api/foxx/service',
            params=params,
            data=data,
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxServiceReplaceError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def delete_service(self, mount, teardown=None):
        """Uninstall a service.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param teardown: Run the service teardown script.
        :type teardown: bool
        :return: True if the service was deleted successfully.
        :rtype: dict
        :raise arango.exceptions.FoxxServiceDeleteError: If delete fails.
        """
        params = {'mount': mount}
        if teardown is not None:
            params['teardown'] = teardown

        request = Request(
            method='delete',
            endpoint='/_api/foxx/service',
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxServiceDeleteError(resp)
            return True

        return self._execute(request, response_handler)

    def config(self, mount):
        """Return service configuration.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Configuration values.
        :rtype: dict
        :raise arango.exceptions.FoxxConfigGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx/configuration',
            params={'mount': mount},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxConfigGetError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def update_config(self, mount, config):
        """Update service configuration.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param config: Configuration values. Omitted options are ignored.
        :type config: dict
        :return: New configuration values.
        :rtype: dict
        :raise arango.exceptions.FoxxConfigUpdateError: If update fails.
        """
        request = Request(
            method='patch',
            endpoint='/_api/foxx/configuration',
            params={'mount': mount},
            data=config
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxConfigUpdateError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def replace_config(self, mount, config):
        """Replace service configuration.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param config: Configuration values. Omitted options are reset to their
            default values or marked as un-configured.
        :type config: dict
        :return: New configuration values.
        :rtype: dict
        :raise arango.exceptions.FoxxConfigReplaceError: If replace fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/foxx/configuration',
            params={'mount': mount},
            data=config
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxConfigReplaceError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def dependencies(self, mount):
        """Return service dependencies.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Dependency settings.
        :rtype: dict
        :raise arango.exceptions.FoxxDependencyGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx/dependencies',
            params={'mount': mount},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxDependencyGetError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def update_dependencies(self, mount, dependencies):
        """Update service dependencies.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param dependencies: Dependencies settings. Omitted ones are ignored.
        :type dependencies: dict
        :return: New dependency settings.
        :rtype: dict
        :raise arango.exceptions.FoxxDependencyUpdateError: If update fails.
        """
        request = Request(
            method='patch',
            endpoint='/_api/foxx/dependencies',
            params={'mount': mount},
            data=dependencies
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxDependencyUpdateError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def replace_dependencies(self, mount, dependencies):
        """Replace service dependencies.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param dependencies: Dependencies settings. Omitted ones are disabled.
        :type dependencies: dict
        :return: New dependency settings.
        :rtype: dict
        :raise arango.exceptions.FoxxDependencyReplaceError: If replace fails.
        """
        request = Request(
            method='put',
            endpoint='/_api/foxx/dependencies',
            params={'mount': mount},
            data=dependencies
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxDependencyReplaceError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def enable_development(self, mount):
        """Put the service into development mode.

        While the service is running in development mode, the it is reloaded
        from the file system and its setup script (if any) is re-executed every
        time the service handles a request.

        In a cluster with multiple coordinators, changes to the filesystem on
        one coordinator is not reflected across other coordinators. The user
        must be aware of this inconsistency in development mode.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Update service metadata.
        :rtype: dict
        :raise arango.exceptions.FoxxDevEnableError: If operation fails.
        """
        request = Request(
            method='post',
            endpoint='/_api/foxx/development',
            params={'mount': mount},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxDevEnableError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def disable_development(self, mount):
        """Put the service into production mode.

        In a cluster with multiple coordinators, this replaces the service on
        all other coordinators with the version on the calling coordinator.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Update service metadata.
        :rtype: dict
        :raise arango.exceptions.FoxxDevDisableError: If operation fails.
        """
        request = Request(
            method='delete',
            endpoint='/_api/foxx/development',
            params={'mount': mount},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxDevDisableError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def readme(self, mount):
        """Return the service readme.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Service readme.
        :rtype: str | unicode
        :raise arango.exceptions.FoxxReadmeGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx/readme',
            params={'mount': mount},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxReadmeGetError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def swagger(self, mount):
        """Return the Swagger API description for a service.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Swagger API description.
        :rtype: dict
        :raise arango.exceptions.FoxxSwaggerGetError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx/swagger',
            params={'mount': mount}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxSwaggerGetError(resp)
            if 'basePath' in resp.body:
                resp.body['base_path'] = resp.body.pop('basePath')
            return resp.body

        return self._execute(request, response_handler)

    def download(self, mount):
        """Download service bundle.

        When development mode is enabled, a new bundle is always created.
        Otherwise, the bundle represents the version of the service installed
        on the ArangoDB instance.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Service bundle in raw string form.
        :rtype: str | unicode
        :raise arango.exceptions.FoxxDownloadError: If download fails.
        """
        request = Request(
            method='post',
            endpoint='/_api/foxx/download',
            params={'mount': mount}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxDownloadError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def commit(self, replace=None):
        """Commit local service state of the coordinator to the database.

        This can be used to resolve service conflicts between coordinators
        that cannot be fixed automatically due to missing data

        :param replace: Overwrite any existing service files in the database.
        :type replace: bool
        :return: True if the commit was successful.
        :rtype: bool
        :raise arango.exceptions.FoxxCommitError: If commit fails.
        """
        params = {}
        if replace is not None:
            params['replace'] = replace

        request = Request(
            method='post',
            endpoint='/_api/foxx/commit',
            params=params
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxCommitError(resp)
            return True

        return self._execute(request, response_handler)

    def scripts(self, mount):
        """List service scripts.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :return: Service scripts.
        :rtype: dict
        :raise arango.exceptions.FoxxScriptListError: If retrieval fails.
        """
        request = Request(
            method='get',
            endpoint='/_api/foxx/scripts',
            params={'mount': mount},
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxScriptListError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def run_script(self, mount, name, arg=None):
        """Run a service script.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param name: Script name.
        :type name: str | unicode
        :param arg: Arbitrary value passed into the script as first arguments.
        :type arg: str | unicode | bool | int | list | dict
        :return: Result of the script, if any.
        :rtype: dict
        :raise arango.exceptions.FoxxScriptRunError: If script fails.
        """
        request = Request(
            method='post',
            endpoint='/_api/foxx/scripts/{}'.format(name),
            params={'mount': mount},
            data=arg or {}
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxScriptRunError(resp)
            return resp.body

        return self._execute(request, response_handler)

    def run_tests(self,
                  mount,
                  reporter='default',
                  idiomatic=None,
                  output_format=None):
        """Run service tests.

        :param mount: Service mount path (e.g "/_admin/aardvark").
        :type mount: str | unicode
        :param reporter: Supported reporters are:

            .. code-block:: none

                "default" : Simple list of test cases.

                "suite"   : Object of test cases nested in suites.

                "stream"  : Raw stream of test results.

                "xunit"   : XUnit/JUnit compatible structure.

                "tap"     : Raw TAP compatible stream.

        :type reporter: str | unicode
        :param idiomatic: Use the matching format for the reporter, regardless
            of the value of parameter **output_format**.
        :type: bool
        :param output_format: Used to further control the result where allowed
            values are "x-ldjson", "xml" and "text". When using the "stream"
            reporter, setting this to "x-ldjson" returns newline-delimited
            JSON stream. When using the "tap" reporter, setting this to "text"
            returns plain text tap report. When using the "xunit" reporter,
            settings this to "xml" returns an XML instead of JSONML.
        :type output_format:
        :return: Reporter output (e.g. raw JSON string, XML, plain text).
        :rtype: str | unicode
        :raise arango.exceptions.FoxxTestRunError: If operation fails.
        """
        params = {'mount': mount, 'reporter': reporter}
        if idiomatic is not None:
            params['idiomatic'] = idiomatic

        headers = {}
        if output_format == 'x-ldjson':
            headers['Accept'] = 'application/x-ldjson'
        elif output_format == 'xml':
            headers['Accept'] = 'application/xml'
        elif output_format == 'text':
            headers['Accept'] = 'text/plain'

        request = Request(
            method='post',
            endpoint='/_api/foxx/tests',
            params=params,
            headers=headers
        )

        def response_handler(resp):
            if not resp.is_success:
                raise FoxxTestRunError(resp)
            return resp.raw_body

        return self._execute(request, response_handler)
