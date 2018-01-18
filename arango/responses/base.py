import json


class Response(object):
    """ArangoDB HTTP response.

    Methods of :class:`arango.http_clients.base.BaseHTTPClient` must return an
    instance of this class.

    :param method: The HTTP method name in lowercase (e.g. "post").
    :type method: str | unicode
    :param url: The request URL
        (e.g. "http://localhost:8529/_db/_system/_api/database").
    :type url: str | unicode
    :param headers: A mapping object (e.g. dict) containing the HTTP headers.
        Must allow case-insensitive key indexing.
    :type headers: collections.MutableMapping
    :param http_code: The HTTP status code.
    :type http_code: int
    :param http_text: The HTTP status text. This is used only for printing
        error messages, and has no strict requirements.
    :type http_text: str | unicode
    :param body: The HTTP response body.
    :type body: str | unicode | dict
    """

    __slots__ = (
        'method',
        'url',
        'headers',
        'status_code',
        'status_text',
        'body',
        'raw_body',
        'error_code',
        'error_message',
    )

    def __init__(self,
                 response,
                 response_mapper):

        processed = response_mapper(response)
        self.method = processed.get('method', None)
        self.url = processed.get('url', None)
        self.headers = processed.get('headers', None)
        self.status_code = processed.get('status_code', None)
        self.status_text = processed.get('status_text', None)
        self.raw_body = None
        self.body = None
        self.error_code = None
        self.error_message = None
        self.update_body(processed.get('body', None))

    def update_body(self, body):
        self.raw_body = body

        try:
            self.body = json.loads(self.raw_body)
        except (ValueError, TypeError):
            self.body = self.raw_body
        if isinstance(self.body, dict):
            self.error_code = self.body.get('errorNum')
            self.error_message = self.body.get('errorMessage')
        else:
            self.error_code = None
            self.error_message = None
