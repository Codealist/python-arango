.. _index-page:

Indexes
-------

**Indexes** can be added to collections to speed up document lookups. Every
collection has a primary hash index on the "_key" field by default. This
index cannot be deleted or modified. Every edge collection has additional edge
index on fields "_from" and "_to".

Here is an example showing how indexes can be added or removed:

.. code-block:: python

    from arango import ArangoClient

    client = ArangoClient()
    db = client.db('my_database')
    cities = db.create_collection('cities')

    # List the indexes in the collection
    cities.indexes()

    # Add a new hash index on fields 'continent' and 'country'
    cities.add_hash_index(fields=['continent', 'country'], unique=True)

    # Add new fulltext indices on fields 'continent' and 'country'
    cities.add_fulltext_index(fields=['continent'])
    cities.add_fulltext_index(fields=['country'])

    # Add a new skiplist index on field 'population'
    cities.add_skiplist_index(fields=['population'], sparse=False)

    # Add a new geo-spatial index on field 'coordinates'
    cities.add_geo_index(fields=['coordinates'])

    # Add a new persistent index on fields 'currency'
    cities.add_persistent_index(fields=['currency'], unique=True, sparse=True)

    # Delete an existing index from the collection
    cities.delete_index('some_index_id')

Refer to :ref:`Collection` class for more details.
