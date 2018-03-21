from __future__ import absolute_import, unicode_literals

import pytest

from arango.database import Database
from arango.exceptions import (
    TransactionBadStateError,
    TransactionExecuteError,
    TransactionJobResultError
)
from arango.utils import is_str
from tests.utils import clean, extract


# noinspection PyUnresolvedReferences
def test_transaction_attributes(db, col, docs):
    with db.begin_transaction() as txn:
        assert is_str(txn.id)
        assert isinstance(txn.db, Database)
        assert txn.db.context == 'transaction'
        assert txn.jobs == []
        assert txn.status == 'pending'
        job = txn.db.collection(col.name).insert_many(docs)

    assert txn.jobs == [job]
    assert txn.status == 'done'
    assert '<Transaction {}>'.format(txn.id) == repr(txn)
    assert '<TransactionJob {}>'.format(job.id) == repr(job)
    assert extract('_key', col.all()) == extract('_key', docs)


def test_transaction_execute_with_result(sys_db, db, col, docs):
    # Test DB level API methods
    with sys_db.begin_transaction(return_result=True) as txn:
        job01 = txn.db.engine()
        job02 = txn.db.version()
        job03 = txn.db.details()
        job04 = txn.db.databases()

    assert job01.result() == sys_db.engine()
    assert job02.result() == sys_db.version()
    assert set(job03.result()).issubset(set(sys_db.details()))
    assert set(job04.result()) == set(sys_db.databases())

    # Test collection level API methods
    with db.begin_transaction(return_result=True) as txn:
        txn_col = txn.db.collection(col.name)
        job01 = txn_col.properties()
        job02 = txn_col.statistics()
        job03 = txn_col.revision()
        job04 = txn_col.checksum()
        job05 = txn_col.unload()
        job06 = txn_col.load()

    assert set(job01.result()).issubset(set(col.properties()))
    assert set(job02.result()).issubset(set(col.statistics()))
    assert is_str(job03.result())  # TODO returning wrong revision
    assert job04.result() == col.checksum()
    assert job05.result() is True
    assert job06.result() is True

    # Test document level API methods
    with db.begin_transaction(return_result=True) as txn:
        txn_col = txn.db.collection(col.name)
        job01 = txn_col.insert_many(docs, sync=True)
        job02 = txn_col.count()
        job03 = txn_col.ids()
        job04 = txn_col.keys()
        job05 = txn_col.has(docs[0])
        job06 = txn_col.get(docs[0])
        job07 = txn_col.get_many(docs)
        job08 = txn_col.find({'text': 'foo'}, offset=1, limit=1)
        job09 = txn_col.all(skip=4, limit=3)
        job10 = txn_col.random()

        new_docs = [{'_key': d['_key'], 'a': 1} for d in docs]
        job11 = txn_col.update_many(new_docs, return_new=True)
        job12 = txn_col.update_match({'a': 1}, {'a': 2})

        new_docs = [{'_key': d['_key'], 'a': 3} for d in docs]
        job13 = txn_col.replace_many(new_docs, return_new=True)
        job14 = txn_col.replace_match({'a': 3}, {'a': 4})

        job15 = txn_col.delete_many(['1', '2'], return_old=True)
        job16 = txn_col.count()
        job17 = txn_col.delete_match({'a': 4})
        job18 = txn_col.count()

        doc = docs[0].copy()
        job19 = txn_col.insert(doc, return_new=True)
        doc['a'] = 1
        job20 = txn_col.update(doc, return_new=True)
        doc['a'] = 2
        job21 = txn_col.replace(doc, return_new=True)
        job22 = txn_col.delete(doc, return_old=True)
        job23 = txn_col.count()

    assert extract('_key', job01.result()) == extract('_key', docs)
    assert job02.result() == len(docs)
    col_ids = ['{}/{}'.format(col.name, doc['_key']) for doc in docs]
    assert sorted(job03.result()) == sorted(col_ids)
    assert sorted(job04.result()) == extract('_key', docs)
    assert job05.result() is True
    assert clean(job06.result()) == clean(docs[0])
    assert clean(job07.result()) == clean(docs)
    assert len(job08.result()) == 1
    assert len(job09.result()) == 2
    assert clean(job10.result()) in clean(docs)
    assert all(d['new']['a'] == 1 for d in job11.result())
    assert job12.result() == len(docs)
    assert all(d['new']['a'] == 3 for d in job13.result())
    assert job14.result() == len(docs)
    assert extract('_key', job15.result()) == ['1', '2']
    assert job16.result() == 4
    assert job17.result() == 4
    assert job18.result() == 0
    assert 'a' not in job19.result()['new']
    assert job20.result()['new']['a'] == 1
    assert job21.result()['new']['a'] == 2
    assert job22.result()['old']['a'] == 2
    assert job23.result() == 0

    # Test vertex level API methods
    with db.begin_transaction(return_result=True) as txn:
        pass

def test_transaction_execute_without_result(db, col, docs):
    with db.begin_transaction(return_result=False) as txn:
        txn_col = txn.db.collection(col.name)
        assert txn_col.insert(docs[0]) is None
        assert txn_col.delete(docs[0]) is None
        assert txn_col.insert(docs[1]) is None
        assert txn_col.delete(docs[1]) is None
        assert txn_col.insert(docs[2]) is None

    assert txn.jobs is None
    assert txn.status == 'done'
    assert extract('_key', col.all()) == extract('_key', [docs[2]])


def test_transaction_empty_commit(db):
    txn = db.begin_transaction()
    assert txn.status == 'pending'

    assert list(txn.commit()) == []
    assert txn.status == 'done'


def test_transaction_double_commit(db, col):
    txn = db.begin_transaction()
    txn.db.collection(col.name).insert({})
    assert txn.status == 'pending'

    # Test first commit
    txn.commit()
    assert txn.status == 'done'
    assert len(col) == 1
    random_doc = col.random()

    # Test second commit which should fail
    with pytest.raises(TransactionBadStateError) as err:
        txn.commit()
    assert txn.status == 'done'
    assert len(col) == 1
    assert col.random() == random_doc
    assert 'committed already' in str(err.value)


def test_transaction_action_after_commit(db, col):
    with db.begin_transaction() as transaction:
        transaction.db.collection(col.name).insert({})

    # Test insert after the transaction has been committed
    with pytest.raises(TransactionBadStateError) as err:
        transaction.db.collection(col.name).insert({})
    assert 'committed already' in str(err.value)
    assert len(col) == 1
    assert transaction.status == 'done'


def test_transaction_execute_error(bad_db, col, docs):
    transaction = bad_db.begin_transaction(return_result=True)
    transaction.db.collection(col.name).insert_many(docs)
    assert transaction.status == 'pending'

    # Test transaction execute with bad credentials
    with pytest.raises(TransactionExecuteError):
        transaction.commit()
    assert len(col) == 0
    assert transaction.status == 'done'


def test_transaction_job_result_not_ready(db, col, docs):
    transaction = db.begin_transaction(return_result=True)
    job = transaction.db.collection(col.name).insert_many(docs)
    assert transaction.status == 'pending'

    # Test get job result before commit with raise_errors set to True
    with pytest.raises(TransactionJobResultError):
        job.result(raise_errors=True)

    # Test get job result before commit with raise_errors set to False
    with pytest.raises(TransactionJobResultError):
        job.result(raise_errors=False)

    # Test commit to make sure it still works after the errors
    assert list(transaction.commit()) == [job]
    assert len(job.result()) == len(docs)
    assert extract('_key', col.all()) == extract('_key', docs)


def test_transaction_execute_raw(db, col, docs):
    doc = docs[0]
    key = doc['_key']
    result = db.execute_transaction(
        command='''
        function (params) {{
            var db = require('internal').db;
            db.{col}.save({{'_key': params.key, 'val': 1}});
            return true;
        }}
        '''.format(col=col.name),
        params={'key': key},
        write=[col.name],
        read=[col.name],
        sync=False,
        timeout=1000,
        max_size=100000,
        allow_implicit=True,
        autocommit_ops=10,
        autocommit_size=10000
    )
    assert result is True
    assert doc in col and col[key]['val'] == 1
