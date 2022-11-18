import json
import cherrypy as cp 

class APIError(cp.HTTPError):
    def __init__(self, status: int = 500, message: str = ''):
        super().__init__(status=status)
        self._error_message = message

    def set_response(self):
        super().set_response()
        response = cp.serving.response
        response.body = json.dumps({'error': self._error_message}).encode()
