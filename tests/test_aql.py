from __future__ import absolute_import, unicode_literals

import pytest

from arango.exceptions import (
    AQLCacheClearError,
    AQLCacheConfigureError,
    AQLCachePropertiesError,
    AQLFunctionCreateError,
    AQLFunctionDeleteError,
    AQLFunctionListError,
    AQLQueryExecuteError,
    AQLQueryExplainError,
    AQLQueryValidateError
)
from tests.utils import extract


def test_aql_attributes(db):
    aql = db.aql
    assert aql.context == 'default'
    assert repr(aql) == '<AQL in {}>'.format(db.name)

    cache = db.aql.cache
    assert cache.context == 'default'
    assert repr(cache) == '<AQLQueryCache in {}>'.format(db.name)


def test_aql_explain(db, col):
    plan_fields = [
        'estimatedNrItems',
        'estimatedCost',
        'rules',
        'variables',
        'collections',
    ]
    # Test explain invalid query
    with pytest.raises(AQLQueryExplainError):
        db.aql.explain('THIS IS AN INVALID QUERY')

    # Test explain valid query with all_plans set to True
    plans = db.aql.explain(
        'FOR d IN {} RETURN d'.format(col.name),
        all_plans=True,
        opt_rules=['-all', '+use-index-range'],
        max_plans=10
    )
    assert len(plans) < 10
    for plan in plans:
        assert all(field in plan for field in plan_fields)

    # Test explain valid query with all_plans set to False
    plan = db.aql.explain(
        'FOR d IN {} RETURN d'.format(col.name),
        all_plans=False,
        opt_rules=['-all', '+use-index-range']
    )
    assert all(field in plan for field in plan_fields)


def test_aql_validate(db, col):
    # Test validate invalid query
    with pytest.raises(AQLQueryValidateError):
        db.aql.validate('THIS IS AN INVALID QUERY')

    # Test validate valid query
    result = db.aql.validate('FOR d IN {} RETURN d'.format(col.name))
    assert 'ast' in result
    assert 'bindVars' in result
    assert 'collections' in result
    assert 'parsed' in result


def test_aql_execute(db, col, docs):
    # Test execute invalid AQL query
    with pytest.raises(AQLQueryExecuteError):
        db.aql.execute('THIS IS AN INVALID QUERY')

    # Test execute valid query
    db.collection(col.name).import_bulk(docs)
    result = db.aql.execute(
        'FOR d IN {} RETURN d'.format(col.name),
        count=True,
        batch_size=1,
        ttl=10,
        optimizer_rules=['+all'],
        cache=True,
        fail_on_warning=False,
        profile=False,
        max_transaction_size=100000,
        max_warning_count=10,
        intermediate_commit_count=1,
        intermediate_commit_size=1000,
        satellite_sync_wait=False
    )
    assert extract('_key', result) == extract('_key', docs)

    # Test execute another valid query
    result = db.aql.execute(
        'FOR d IN {} FILTER d.text == @text RETURN d'.format(col.name),
        bind_vars={'text': 'foo'},
        count=True,
        full_count=True,
        max_plans=100
    )
    assert extract('_key', result) == extract('_key', docs[:3])


def test_aql_function_management(db, bad_db):
    fn_group = 'functions::temperature'
    fn_name_1 = 'functions::temperature::celsius_to_fahrenheit'
    fn_body_1 = 'function (celsius) { return celsius * 1.8 + 32; }'
    fn_name_2 = 'functions::temperature::fahrenheit_to_celsius'
    fn_body_2 = 'function (fahrenheit) { return (fahrenheit - 32) / 1.8; }'
    bad_fn_name = 'functions::temperature::should_not_exist'
    bad_fn_body = 'function (celsius) { invalid syntax }'

    # Test list AQL functions with bad credentials
    with pytest.raises(AQLFunctionListError):
        bad_db.aql.functions()

    # Test create invalid AQL function
    with pytest.raises(AQLFunctionCreateError):
        db.aql.create_function(bad_fn_name, bad_fn_body)

    # Test create AQL function one
    db.aql.create_function(fn_name_1, fn_body_1)
    assert db.aql.functions() == {fn_name_1: fn_body_1}

    # Test create AQL function one again (idempotency)
    db.aql.create_function(fn_name_1, fn_body_1)
    assert db.aql.functions() == {fn_name_1: fn_body_1}

    # Test create AQL function two
    db.aql.create_function(fn_name_2, fn_body_2)
    assert db.aql.functions() == {fn_name_1: fn_body_1, fn_name_2: fn_body_2}

    # Test delete AQL function one
    assert db.aql.delete_function(fn_name_1) is True
    assert db.aql.functions() == {fn_name_2: fn_body_2}

    # Test missing AQL function
    with pytest.raises(AQLFunctionDeleteError):
        db.aql.delete_function(fn_name_1)
    assert db.aql.delete_function(fn_name_1, ignore_missing=True) is False
    assert db.aql.functions() == {fn_name_2: fn_body_2}

    # Test delete AQL function group
    assert db.aql.delete_function(fn_group, group=True) is True
    assert db.aql.functions() == {}


def test_aql_cache_properties(db, bad_db):
    # Test get AQL cache properties
    properties = db.aql.cache.properties()
    assert 'mode' in properties
    assert 'limit' in properties

    # Test get AQL cache properties with bad credentials
    with pytest.raises(AQLCachePropertiesError):
        bad_db.aql.cache.properties()


def test_aql_cache_configure(db, bad_db):
    # Test get AQL cache configure
    properties = db.aql.cache.configure(mode='on', limit=100)
    assert properties['mode'] == 'on'
    assert properties['limit'] == 100

    properties = db.aql.cache.properties()
    assert properties['mode'] == 'on'
    assert properties['limit'] == 100

    # Test get AQL cache configure with bad credentials
    with pytest.raises(AQLCacheConfigureError):
        bad_db.aql.cache.configure(mode='on')


def test_aql_cache_clear(db, bad_db):
    # Test get AQL cache clear
    result = db.aql.cache.clear()
    assert isinstance(result, bool)

    # Test get AQL cache clear with bad credentials
    with pytest.raises(AQLCacheClearError):
        bad_db.aql.cache.clear()
