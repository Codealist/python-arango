from arango.utils import RLock
from arango.jobs import BaseJob


class BatchJob(BaseJob):
    """ArangoDB batch job which holds the result of an API request.

    A batch job tracks the status of a queued API request and its result.
    """

    def __init__(self, handler, response=None):
        BaseJob.__init__(self, handler, response=response, job_type='batch')
        self._lock = RLock()

    def update(self, status, response=None):
        """Update the status and the response of the batch job.

        This method designed to be used internally only.

        :param status: The status of the job
        :type status: str
        :param response: The response to the job
        :type response: arango.responses.base.Response
        """
        with self._lock:
            return BaseJob.update(self, status, response)

    def status(self):
        """Return the status of the batch job.

        :return: The batch job status, which can be "pending" (the job is
            still waiting to be committed), "done" (the job completed) or
            "error" (the job raised an exception)
        :rtype: str | unicode
        """
        with self._lock:
            return BaseJob.status(self)

    def result(self, raise_errors=False):
        """Return the result of the job or its error.

        :return: The result of the batch job if the job is successful
        :rtype: object
        :raise ArangoError: If the batch job failed
        """
        with self._lock:
            return BaseJob.result(self)
