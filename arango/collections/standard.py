from __future__ import absolute_import, unicode_literals

from json import dumps

from six import string_types

from arango import Request
from arango.collections import BaseCollection
from arango.exceptions import (
    DocumentDeleteError,
    DocumentGetError,
    DocumentInsertError,
    DocumentReplaceError,
    DocumentRevisionError,
    DocumentUpdateError
)
from arango.utils import HTTP_OK


class Collection(BaseCollection):
    """ArangoDB standard collection.

    A collection consists of documents. It is uniquely identified by its name,
    which must consist only of alphanumeric, hyphen and underscore characters.

    Be default, collections use the traditional key generator, which generates
    key values in a non-deterministic fashion. A deterministic, auto-increment
    key generator is available as well.

    :param requester: ArangoDB API requester object.
    :type requester: arango.requesters.Requester
    :param name: The name of the collection.
    :type name: str | unicode
    """

    def __init__(self, requester, name):
        super(Collection, self).__init__(requester, name)

    def __repr__(self):
        return '<ArangoDB collection "{}">'.format(self._name)

    def get(self, key, rev=None, match_rev=True):
        """Retrieve a document by its key.

        :param key: The document key.
        :type key: str | unicode
        :param rev: The document revision to be compared against the revision
            of the target document.
        :type rev: str | unicode
        :param match_rev: This parameter applies only when **rev** is given. If
            set to True, ensure that the document revision matches the value of
            **rev**. Otherwise, ensure that they do not match.
        :type match_rev: bool
        :return: The document, or None if it is missing.
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If **rev** is given and
            it does not match the target document revision.
        :raise arango.exceptions.DocumentGetError: If the retrieval fails.
        """
        headers = {}
        if rev is not None:
            if match_rev:
                headers['If-Match'] = rev
            else:
                headers['If-None-Match'] = rev

        request = Request(
            method='get',
            endpoint='/_api/document/{}/{}'.format(self._name, key),
            headers=headers
        )

        def handler(res):
            if res.status_code in {304, 412}:
                raise DocumentRevisionError(res)
            elif res.status_code == 404 and res.error_code == 1202:
                return None
            elif res.status_code in HTTP_OK:
                return res.body
            raise DocumentGetError(res)

        return self._execute_request(request, handler)

    def insert(self, document, return_new=False, sync=None, silent=False):
        """Insert a new document.

        :param document: The document to insert.
        :type document: dict
        :param return_new: If set to True, full body of the new document is
            included in the returned result.
        :type return_new: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no metadata is returned in the result.
            This can be used to save some network traffic.
        :type silent: bool
        :return: The result of the insert (e.g. document key, revision).
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If the insert fails.

        .. note::
            If the "_key" field is present in **document**, its value is
            used as the key of the new document. If not present, the key is
            auto-generated.

        .. note::
            The "_id" and "_rev" fields are ignored if present in **document**.

        .. note::
            Parameter **return_new** has no effect in transactions.
        """
        params = {
            'returnNew': return_new,
            'silent': silent,
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            command = 'db.{}.insert({},{})'.format(
                self._name,
                dumps(document),
                dumps(params)
            )

        request = Request(
            method='post',
            endpoint='/_api/document/{}'.format(self._name),
            data=document,
            params=params,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            if res.status_code == 202:
                res.body['sync'] = False
            else:
                res.body['sync'] = True
            return res.body

        return self._execute_request(request, handler)

    def insert_many(self,
                    documents,
                    return_new=False,
                    sync=None,
                    silent=False):
        """Insert multiple documents into the collection.

        :param documents: The list of the new documents to insert.
        :type documents: [dict]
        :param return_new: If set to True, bodies of the new documents are
            included in the returned result.
        :type return_new: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no metadata is returned in the result.
            This can be used to save some network traffic.
        :type silent: bool
        :return: The result of the insert (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If the insert fails.

        .. note::
            If the "_key" fields are present in the entries in **documents**,
            their values are used as the keys of the new documents. Otherwise
            the keys are auto-generated.

        .. note::
            The "_id" and "_rev" fields are ignored if found in **documents**.

        .. note::
            Parameter **return_new** has no effect in transactions.
        """
        params = {
            'returnNew': return_new,
            'silent': silent,
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            command = 'db.{}.insert({},{})'.format(
                self._name,
                dumps(documents),
                dumps(params)
            )

        request = Request(
            method='post',
            endpoint='/_api/document/{}'.format(self._name),
            data=documents,
            params=params,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)

            results = []
            for result in res.body:
                if '_id' not in result:
                    result = DocumentInsertError(
                        res.update_body(result)
                    )
                elif res.status_code == 202:
                    result['sync'] = False
                elif res.status_code:
                    result['sync'] = True
                results.append(result)
            return results

        return self._execute_request(request, handler)

    def update(self,
               document,
               merge=True,
               keep_none=True,
               return_new=False,
               return_old=False,
               check_rev=False,
               sync=None,
               silent=False):
        """Update a document.

        :param document: Partial or full document with the updated values.
        :type document: dict
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new one overwriting the old one.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: If set to True, full body of the new document is
            included in the returned result.
        :type return_new: bool
        :param return_old: If set to True, full body of the old document is
            included in the returned result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" field in **document**
            is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no metadata is returned in the result.
            This can be used to save some network traffic.
        :type silent: bool
        :return: The result of the update (e.g. document key, revision).
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If "_rev" key is
            present and its value does not match the revision of the target
            document.
        :raise arango.exceptions.DocumentUpdateError: If the update fails.

        .. note::
            The "_key" field must be present in **document**.

        .. note::
            Parameters **return_new** and **return_old** have no effect in
            transactions.
        """
        params = {
            'keepNull': keep_none,
            'mergeObjects': merge,
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev,
            'silent': silent
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            if not check_rev:
                document.pop('_rev', None)
            documents_str = dumps(document)
            command = 'db.{}.update({},{},{})'.format(
                self._name,
                documents_str,
                documents_str,
                dumps(params)
            )

        request = Request(
            method='patch',
            endpoint='/_api/document/{}/{}'.format(
                self._name, document['_key']
            ),
            data=document,
            params=params,
            command=command
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            elif res.status_code == 202:
                res.body['sync'] = False
            else:
                res.body['sync'] = True
            res.body['_old_rev'] = res.body.pop('_oldRev')
            return res.body

        return self._execute_request(request, handler)

    def update_many(self,
                    documents,
                    merge=True,
                    keep_none=True,
                    return_new=False,
                    return_old=False,
                    check_rev=False,
                    sync=None):
        """Update multiple documents.

        :param documents: Partial or full documents with the updated values.
        :type documents: [dict]
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new ones overwriting the old ones.
        :type merge: bool
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param return_new: If set to True, full bodies of the new documents are
            included in the returned result.
        :type return_new: bool
        :param return_old: If set to True, full bodies of the old documents are
            included in the returned result.
        :type return_old: bool
       :param check_rev: If set to True, the "_rev" fields in **documents**
            are compared against the revisions of the target documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The result of the update (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If "_rev" keys are
            present and their values do not match the revisions of the
            respective target documents.
        :raise arango.exceptions.DocumentUpdateError: If the update fails.

        .. note::
            The "_key" fields must be present in **documents**.

        .. note::
            Parameters **return_new** and **return_old** have no effect in
            transactions

        .. warning::
            The returned details (whose size scales with the number of updated
            documents) are all brought into memory.
        """
        params = {
            'keepNull': keep_none,
            'mergeObjects': merge,
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            documents_str = dumps(documents)
            command = 'db.{}.update({},{},{})'.format(
                self._name,
                documents_str,
                documents_str,
                dumps(params)
            )

        request = Request(
            method='patch',
            endpoint='/_api/document/{}'.format(self._name),
            data=documents,
            params=params,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)

            results = []
            for result in res.body:
                # TODO this is not clean
                if '_id' not in result:
                    # An error occurred with this particular document
                    err = res.update_body(result)
                    # Single out revision error
                    if result['errorNum'] == 1200:
                        result = DocumentRevisionError(err)
                    else:
                        result = DocumentUpdateError(err)
                else:
                    if res.status_code == 202:
                        result['sync'] = False
                    elif res.status_code:
                        result['sync'] = True
                    result['_old_rev'] = result.pop('_oldRev')
                results.append(result)

            return results

        return self._execute_request(request, handler)

    def update_match(self,
                     filters,
                     body,
                     limit=None,
                     keep_none=True,
                     sync=None,
                     merge=True):
        """Update matching documents.

        :param filters: The document filters.
        :type filters: dict
        :param body: The document body with the updates.
        :type body: dict
        :param limit: The maximum number of documents to update. If the limit
            is lower than the number of matched documents, random documents
            are chosen.
        :type limit: int
        :param keep_none: If set to True, fields with value None are retained
            in the document. Otherwise, they are removed completely.
        :type keep_none: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The number of documents updated.
        :rtype: int
        :param merge: If set to True, sub-dictionaries are merged instead of
            the new ones overwriting the old ones.
        :type merge: bool
        :raise arango.exceptions.DocumentUpdateError: If the update fails.

        .. note::
            Parameter **limit** is not supported on sharded collections.
        """
        data = {
            'collection': self._name,
            'example': filters,
            'newValue': body,
            'keepNull': keep_none,
            'mergeObjects': merge
        }
        if limit is not None:
            data['limit'] = limit
        if sync is not None:
            data['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            command = 'db.{}.updateByExample({},{},{})'.format(
                self._name,
                dumps(filters),
                dumps(body),
                dumps(data)
            )

        request = Request(
            method='put',
            endpoint='/_api/simple/update-by-example',
            data=data,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            return res.body['updated']

        return self._execute_request(request, handler)

    def replace(self,
                document,
                return_new=False,
                return_old=False,
                check_rev=False,
                sync=None,
                silent=False):
        """Replace a document.

        :param document: The new document to replace the old one with.
        :type document: dict
        :param return_new: If set to True, full body of the new document is
            included in the returned result.
        :type return_new: bool
        :param return_old: If set to True, full body of the old document is
            included in the returned result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" field in **document**
            is compared against the revision of the target document.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no metadata is returned in the result.
            This can be used to save some network traffic.
        :type silent: bool
        :return: The result of the replace (e.g. document key, revision).
        :rtype: dict
        :raise arango.exceptions.DocumentRevisionError: If "_rev" key is
            present and its value does not match the revision of the target
            document.
        :raise arango.exceptions.DocumentReplaceError: If the replace fails.

        .. note::
            The "_key" field must be present in **document**. For edge
            documents the "_from" and "_to" fields must also be present.

        .. note::
            Parameters **return_new** and **return_old** have no effect in
            transactions.
        """
        params = {
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev,
            'silent': silent
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            documents_str = dumps(document)
            command = 'db.{}.replace({},{},{})'.format(
                self._name,
                documents_str,
                documents_str,
                dumps(params)
            )

        request = Request(
            method='put',
            endpoint='/_api/document/{}/{}'.format(
                self._name, document['_key']
            ),
            params=params,
            data=document,
            command=command
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            if res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            if res.status_code == 202:
                res.body['sync'] = False
            else:
                res.body['sync'] = True
            res.body['_old_rev'] = res.body.pop('_oldRev')
            return res.body

        return self._execute_request(request, handler)

    def replace_many(self,
                     documents,
                     return_new=False,
                     return_old=False,
                     check_rev=False,
                     sync=None):
        """Replace multiple documents.

        :param documents: The new documents to replace the old ones with.
        :type documents: [dict]
        :param return_new: If set to True, full bodies of the new documents are
            included in the returned result.
        :type return_new: bool
        :param return_old: If set to True, full bodies of the old documents are
            included in the returned result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" fields in **documents**
            are compared against the revisions of the target documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The result of the replace (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentReplaceError: If the replace fails.

        .. note::
            The "_key" fields must be present in **documents**. For edge
            documents the "_from" and "_to" fields must also be present.

        .. note::
            Parameters **return_new** and **return_old** have no effect in
            transactions.

        .. warning::
            The returned details (whose size scales with the number of replaced
            documents) are all brought into memory.
        """
        params = {
            'returnNew': return_new,
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            documents_str = dumps(documents)
            command = 'db.{}.replace({},{},{})'.format(
                self._name,
                documents_str,
                documents_str,
                dumps(params)
            )

        request = Request(
            method='put',
            endpoint='/_api/document/{}'.format(self._name),
            params=params,
            data=documents,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)

            results = []
            for result in res.body:
                # TODO this is not clean
                if '_id' not in result:
                    # An error occurred with this particular document
                    err = res.update_body(result)
                    # Single out revision error
                    if result['errorNum'] == 1200:
                        result = DocumentRevisionError(err)
                    else:
                        result = DocumentReplaceError(err)
                else:
                    if res.status_code == 202:
                        result['sync'] = False
                    elif res.status_code:
                        result['sync'] = True
                    result['_old_rev'] = result.pop('_oldRev')
                results.append(result)

            return results

        return self._execute_request(request, handler)

    def replace_match(self, filters, body, limit=None, sync=None):
        """Replace matching documents.

        :param filters: The document filters.
        :type filters: dict
        :param body: The new document body.
        :type body: dict
        :param limit: The maximum number of documents to replace. If the limit
            is lower than the number of matched documents, random documents
            are chosen.
        :type limit: int
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The number of documents replaced.
        :rtype: int
        :raise arango.exceptions.DocumentReplaceError: If the replace fails.
        """
        data = {
            'collection': self._name,
            'example': filters,
            'newValue': body
        }
        if limit is not None:
            data['limit'] = limit
        if sync is not None:
            data['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            command = 'db.{}.replaceByExample({},{},{})'.format(
                self._name,
                dumps(filters),
                dumps(body),
                dumps(data)
            )

        request = Request(
            method='put',
            endpoint='/_api/simple/replace-by-example',
            data=data,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            return res.body['replaced']

        return self._execute_request(request, handler)

    def delete(self,
               document,
               ignore_missing=False,
               return_old=False,
               check_rev=False,
               sync=None,
               silent=False):
        """Delete a document.

        :param document: The document to delete or its key.
        :type document: dict | str | unicode
        :param ignore_missing: Do not raise an exception on missing document.
        :type ignore_missing: bool
        :param return_old: If set to True, full body of the old document is
            included in the returned result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" field in **document**
            is compared against the revision of the target document. (this only
            applicable when **document** is an actual document and not a key).
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :param silent: If set to True, no metadata is returned in the result.
            This can be used to save some network traffic.
        :type silent: bool
        :return: The results of the delete (e.g. document key, new revision),
            or False if the document was missing but ignored.
        :rtype: dict | bool
        :raise arango.exceptions.DocumentRevisionError: If the "_rev" field is
            in **document** and its value does not match the revision of the
            target document.
        :raise arango.exceptions.DocumentDeleteError: If the delete fails.

        .. note::
            If **document** is a document body, it must have the "_key" field.

        .. note::
            Parameter **return_old** has no effect in transactions.
        """
        params = {
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev,
            'silent': silent
        }
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        if isinstance(document, string_types):
            document = {'_key': document}
        else:
            revision = document.get('_rev')
            if revision is not None:
                headers['If-Match'] = revision

        if self._requester.type != 'transaction':
            command = None
        else:
            command = 'db.{}.remove({},{})'.format(
                self._name,
                dumps(document),
                dumps(params)
            )

        request = Request(
            method='delete',
            endpoint='/_api/document/{}/{}'.format(
                self._name, document['_key']
            ),
            params=params,
            headers=headers,
            command=command
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            if res.status_code == 202:
                res.body['sync'] = False
            else:
                res.body['sync'] = True
            return res.body

        return self._execute_request(request, handler)

    def delete_many(self,
                    documents,
                    return_old=False,
                    check_rev=False,
                    sync=None):
        """Delete multiple documents.

        :param documents: The list of documents or keys to delete.
        :type documents: [dict] | [str | unicode]
        :param return_old: If set to True, full bodies of the old documents are
            included in the returned result.
        :type return_old: bool
        :param check_rev: If set to True, the "_rev" fields in **documents**
            are compared against the revisions of the target documents.
        :type check_rev: bool
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The result of the delete (e.g. document keys, revisions).
        :rtype: dict
        :raise arango.exceptions.DocumentDeleteError: If the delete fails.

        .. note::
            If an entry in **documents** is a dictionary it must have the
            "_key" field.

        .. note::
            Parameter **return_old** has no effect in transactions.
        """
        sanitized_documents = []
        for document in documents:
            if isinstance(document, string_types):
                sanitized_documents.append({'_key': document})
            else:
                sanitized_documents.append(document)

        params = {
            'returnOld': return_old,
            'ignoreRevs': not check_rev,
            'overwrite': not check_rev
        }
        if sync is not None:
            params['waitForSync'] = sync

        if self._requester.type != 'transaction':
            command = None
        else:
            command = 'db.{}.remove({},{})'.format(
                self._name,
                dumps(sanitized_documents),
                dumps(params)
            )

        request = Request(
            method='delete',
            endpoint='/_api/document/{}'.format(self._name),
            params=params,
            data=sanitized_documents,
            command=command
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)

            results = []
            for result in res.body:
                if '_id' not in result:
                    # An error occurred with this particular document
                    err = res.update_body(result)
                    # Single out revision errors
                    if result['errorNum'] == 1200:
                        result = DocumentRevisionError(err)
                    else:
                        result = DocumentDeleteError(err)
                else:
                    if res.status_code == 202:
                        result['sync'] = False
                    elif res.status_code:
                        result['sync'] = True
                results.append(result)

            return results

        return self._execute_request(request, handler)

    def delete_match(self, filters, limit=None, sync=None):
        """Delete matching documents.

        :param filters: The document filters.
        :type filters: dict
        :param limit: The maximum number of documents to delete. If the limit
            is lower than the number of matched documents, random documents
            are chosen.
        :type limit: int
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The number of documents deleted.
        :rtype: dict
        :raise arango.exceptions.DocumentDeleteError: If the delete fails.
        """
        data = {'collection': self._name, 'example': filters}
        if sync is not None:
            data['waitForSync'] = sync
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-example',
            data=data,
            command='db.{}.removeByExample({}, {})'.format(
                self._name,
                dumps(filters),
                dumps(data)
            )
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body['deleted']

        return self._execute_request(request, handler)

    def import_bulk(self,
                    documents,
                    halt_on_error=True,
                    details=True,
                    from_prefix=None,
                    to_prefix=None,
                    overwrite=None,
                    on_duplicate=None,
                    sync=None):
        """Insert multiple documents into the collection.

        This is faster than :func:`arango.collections.Collection.insert_many`
        but does not return as much information.

        :param documents: The list of the new documents to insert.
        :type documents: [dict]
        :param halt_on_error: Halt the entire import on an error.
        :type halt_on_error: bool
        :param details: If set to True, the returned result will include an
            additional list of detailed error messages.
        :type details: bool
        :param from_prefix: The string prefix to prepend to the "_from" field
            of each edge document inserted (only applies to edge collections).
        :type from_prefix: str | unicode
        :param to_prefix: The string prefix to prepend to the "_to" field
            of each edge document inserted (only applies to edge collections).
        :type to_prefix: str | unicode
        :param overwrite: If set to True, all existing documents in the
            collection are removed prior to the import. Indexes are still
            preserved.
        :type overwrite: bool
        :param on_duplicate: The action to take on unique key constraint
            violations, where possible values are:

            .. code-block:: none

                "error"   : Do not import the new documents and count them as
                            errors (this is the default).

                "update"  : Update the existing documents while preserving any
                            fields missing in the new ones.

                "replace" : Replace the existing documents with the new ones.

                "ignore"  : Do not import the new documents and count them as
                            ignored, as opposed to counting them as errors.

        :type on_duplicate: str | unicode
        :param sync: Block until the operation is synchronized to disk.
        :type sync: bool
        :return: The result of the bulk import.
        :rtype: dict
        :raise arango.exceptions.DocumentInsertError: If the import fails.

        .. note::
            Parameters **from_prefix** and **to_prefix** only work for edge
            collections. When the prefix is prepended, it is followed by a
            "/" character. For example, prefix "foo" prepended to an
            edge document with "_from": "bar" will result in a new value
            "_from": "foo/bar".

        .. note::
            Parameter **on_duplicate** actions "update", "replace" and "ignore"
            only applies on **documents** with the "_key" fields.

        .. note::
            The "_id" and "_rev" fields are ignored if found in  **documents**.

        .. warning::
            Parameter **on_duplicate** actions "update" and "replace" may fail
            on secondary unique key constraint violations.
        """
        params = {
            'type': 'array',
            'collection': self._name,
            'complete': halt_on_error,
            'details': details,
        }
        if halt_on_error is not None:
            params['complete'] = halt_on_error
        if details is not None:
            params['details'] = details
        if from_prefix is not None:
            params['fromPrefix'] = from_prefix
        if to_prefix is not None:
            params['toPrefix'] = to_prefix
        if overwrite is not None:
            params['overwrite'] = overwrite
        if on_duplicate is not None:
            params['onDuplicate'] = on_duplicate
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/import',
            data=documents,
            params=params
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body

        return self._execute_request(request, handler)
