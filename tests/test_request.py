from __future__ import absolute_import, unicode_literals

from arango.request import Request


def test_request_no_data():
    request = Request(
        method='post',
        endpoint='/_api/test',
        params={'bool': True},
        headers={'foo': 'bar'}
    )
    assert str(request) == '\r\n'.join([
        'post /_api/test?bool=1 HTTP/1.1',
        'foo: bar',
        'content-type: application/json',
        'charset: utf-8',
    ])
    assert request.method == 'post'
    assert request.endpoint == '/_api/test'
    assert request.params == {'bool': 1}
    assert request.headers == {
        'foo': 'bar',
        'charset': 'utf-8',
        'content-type': 'application/json'
    }
    assert request.data is None
    assert request.command is None
    assert request.read is None
    assert request.write is None


def test_request_string_data():
    request = Request(
        method='post',
        endpoint='/_api/test',
        params={'bool': True},
        headers={'foo': 'bar'},
        data='test'
    )
    assert str(request) == '\r\n'.join([
        'post /_api/test?bool=1 HTTP/1.1',
        'foo: bar',
        'content-type: application/json',
        'charset: utf-8',
        '\r\ntest',
    ])
    assert request.method == 'post'
    assert request.endpoint == '/_api/test'
    assert request.params == {'bool': 1}
    assert request.headers == {
        'foo': 'bar',
        'charset': 'utf-8',
        'content-type': 'application/json'
    }
    assert request.data == 'test'
    assert request.command is None
    assert request.read is None
    assert request.write is None


def test_request_json_data():
    request = Request(
        method='post',
        endpoint='/_api/test',
        params={'bool': True},
        headers={'foo': 'bar'},
        data={'baz': 'qux'}
    )
    assert str(request) == '\r\n'.join([
        'post /_api/test?bool=1 HTTP/1.1',
        'foo: bar',
        'content-type: application/json',
        'charset: utf-8',
        '\r\n{"baz": "qux"}',
    ])
    assert request.method == 'post'
    assert request.endpoint == '/_api/test'
    assert request.params == {'bool': 1}
    assert request.headers == {
        'foo': 'bar',
        'charset': 'utf-8',
        'content-type': 'application/json'
    }
    assert request.data == '{"baz": "qux"}'
    assert request.command is None
    assert request.read is None
    assert request.write is None


def test_request_transaction_data():
    request = Request(
        method='post',
        endpoint='/_api/test',
        params={'bool': True},
        headers={'foo': 'bar'},
        data={'baz': 'qux'},
        command='return 1',
        read='one',
        write='two',
    )
    assert str(request) == '\r\n'.join([
        'post /_api/test?bool=1 HTTP/1.1',
        'foo: bar',
        'content-type: application/json',
        'charset: utf-8',
        '\r\n{"baz": "qux"}',
    ])
    assert request.method == 'post'
    assert request.endpoint == '/_api/test'
    assert request.params == {'bool': 1}
    assert request.headers == {
        'foo': 'bar',
        'charset': 'utf-8',
        'content-type': 'application/json'
    }
    assert request.data == '{"baz": "qux"}'
    assert request.command == 'return 1'
    assert request.read == 'one'
    assert request.write == 'two'
