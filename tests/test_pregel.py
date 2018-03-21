from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.exceptions import (
    PregelJobCreateError,
    PregelJobGetError,
    PregelJobDeleteError
)
from tests.utils import generate_string


def test_pregel_management(db, graph):
    # Test create pregel job
    job_id = db.create_pregel_job(
        'pagerank',
        graph.name,
        store=False,
        max_gss=100,
        thread_count=1,
        async_mode=False,
        result_field='result',
        alg_params={'threshold': 0.000001}
    )
    assert isinstance(job_id, int)

    # Test create pregel job with unsupported algorithm
    with pytest.raises(PregelJobCreateError):
        db.create_pregel_job('invalid', graph.name)

    # Test get existing pregel job
    job = db.pregel_job(job_id)
    assert isinstance(job['state'], string_types)
    assert isinstance(job['aggregators'], dict)
    assert isinstance(job['gss'], int)
    assert isinstance(job['received_count'], int)
    assert isinstance(job['send_count'], int)
    assert isinstance(job['total_runtime'], float)

    # Test get missing pregel job
    with pytest.raises(PregelJobGetError):
        db.pregel_job(generate_string())

    # Test delete existing pregel job
    assert db.delete_pregel_job(job_id) is True
    with pytest.raises(PregelJobGetError):
        db.pregel_job(job_id)

    # Test delete missing pregel job
    with pytest.raises(PregelJobDeleteError):
        db.delete_pregel_job(generate_string())
