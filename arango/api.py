class APIWrapper:

    def __init__(self, requester):
        self._requester = requester

    def _execute_request(self, request, response_handler):
        return self._requester.execute_request(request, response_handler)
