from http.server import HTTPServer, BaseHTTPRequestHandler
from http import HTTPStatus
from threading import Thread
from abc import abstractmethod
from core.logger import Logger, ModuleLogger
from core.appmodule import AppModule
from core.utils import ModuleStartupError


# All request handlers except for OPTIONS are expected to return (status_code, output_headers, response_body)
RequestHandlerResult = tuple[int, dict, str]


class MyHTTPRequestHandler(BaseHTTPRequestHandler):
    """Auxiliary class that called by the HTTP server to handle requests"""

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', '0'))
        if content_length > 0:
            body = self.rfile.read(content_length)
            if self.server.on_post:
                status_code, out_headers, out_body = self.server.on_post(self.path, self.headers, body)
                self.send_response(status_code)
                for key, value in out_headers.items():
                    self.send_header(key, value)
                self.end_headers()
                if len(out_body) > 0:
                    self.wfile.write(out_body.encode())
            else:
                log_msg = 'on_post callback is not set in HTTP Server instance'
                if self.server._logger:
                    self.server._logger.warning(log_msg)
                else:
                    print(log_msg)
                self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
                self.end_headers()
        else:
            log_msg = 'Received request does not contain Content-length or it is 0'
            if self.server._logger:
                self.server._logger.warning(log_msg)
            else:
                print(log_msg)
            self.send_response(HTTPStatus.BAD_REQUEST, 'Content-length is 0 or absent')
            self.end_headers()

    def do_GET(self):
        content_length = int(self.headers.get('Content-Length', '0'))
        if content_length > 0:
            body = self.rfile.read(content_length)
        else:
            body = ''
        if self.server.on_get:
            status_code, out_headers, out_body = self.server.on_get(self.path, self.headers, body)
            self.send_response(status_code)
            for key, value in out_headers.items():
                self.send_header(key, value)
            self.end_headers()
            if len(out_body) > 0:
                self.wfile.write(out_body.encode())
        else:
            log_msg = 'on_get callback is not set in HTTP Server instance'
            if self.server._logger:
                self.server._logger.warning(log_msg)
            else:
                print(log_msg)
            self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
            self.end_headers()

    def do_PUT(self):
        content_length = int(self.headers.get('Content-Length', '0'))
        if content_length > 0:
            body = self.rfile.read(content_length)
            if self.server.on_put:
                status_code, out_headers, out_body = self.server.on_put(self.path, self.headers, body)
                self.send_response(status_code)
                for key, value in out_headers.items():
                    self.send_header(key, value)
                self.end_headers()
                if len(out_body) > 0:
                    self.wfile.write(out_body.encode())
            else:
                log_msg = 'on_put callback is not set in HTTP Server instance'
                if self.server._logger:
                    self.server._logger.warning(log_msg)
                else:
                    print(log_msg)
                self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
                self.end_headers()
        else:
            log_msg = 'Received request does not contain Content-length or it is 0'
            if self.server._logger:
                self.server._logger.warning(log_msg)
            else:
                print(log_msg)
            self.send_response(HTTPStatus.BAD_REQUEST, 'Content-length is 0 or absent')
            self.end_headers()

    def do_DELETE(self):
        if self.server.on_delete:
                status_code, out_headers, out_body = self.server.on_delete(self.path, self.headers)
                self.send_response(status_code)
                for key, value in out_headers.items():
                    self.send_header(key, value)
                self.end_headers()
        else:
            log_msg = 'on_delete callback is not set in HTTP Server instance'
            if self.server._logger:
                self.server._logger.warning(log_msg)
            else:
                print(log_msg)
            self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
            self.end_headers()

    def do_OPTIONS(self):
        if self.server.on_options:
            status_code, out_headers = self.server.on_options(self.path, self.headers)
            self.send_response(status_code)
            for key, value in out_headers.items():
                self.send_header(key, value)
            self.end_headers()
        else:
            log_msg = 'on_options callback is not set in HTTP Server instance'
            if self.server._logger:
                self.server._logger.warning(log_msg)
            else:
                print(log_msg)
            self.send_response(HTTPStatus.METHOD_NOT_ALLOWED)
            self.end_headers()


class MyHTTPServer(HTTPServer):
    """Auxiliary subclass of the standard HTTPServer class
       Adds callbacks to the original server to be called by the request handler
       when a GET, POST, PUT or OPTIONS request is received passing the request's body
       in a callback's parameter.
       Also adds centralized logging facility
    """
    @property
    def on_get(self):
        """If implemented, called when a GET request is received"""
        return self._on_get

    @on_get.setter
    def on_get(self, func):
        """Define the get callback.
           Expected signature get_callback(path, headers, body)->RequestHandlerResult
        """
        self._on_get = func

    @property
    def on_post(self):
        """If implemented, called when a POST request is received"""
        return self._on_post

    @on_post.setter
    def on_post(self, func):
        """Define the post callback.
           Expected signature post_callback(path, headers, body)->RequestHandlerResult
        """
        self._on_post = func

    @property
    def on_put(self):
        """If implemented, called when a PUT request is received"""
        return self._on_put

    @on_put.setter
    def on_put(self, func):
        """Define the put callback.
           Expected signature put_callback(path, headers, body)->RequestHandlerResult
        """
        self._on_put = func

    @property
    def on_delete(self):
        """If implemented, called when a DELETE request is received"""
        return self._on_delete

    @on_delete.setter
    def on_delete(self, func):
        """Define the delete callback.
           Expected signature put_callback(path, headers)->RequestHandlerResult
        """
        self._on_delete = func

    @property
    def on_options(self):
        """If implemented, called when an OPTIONS request is received"""
        return self._on_options

    @on_options.setter
    def on_options(self, func):
        """Define the options callback.
           Expected signature options_callback(path, headers)->tuple(status_code, headers)
        """
        self._on_options = func

    def set_logger(self, logger: ModuleLogger):
        self._logger = logger


class RestServerBase(AppModule):
    """Acts as an HTTP server waiting for requests.
       Assumed to be subclassed having own implementations of the callbacks, called by the requests handler.
    """
    def __init__(self, modname: str, config_data: dict, logger: Logger):
        super().__init__(modname, config_data, logger)
        self._work_thread: Thread = None
        self._httpd: MyHTTPServer = None

    def start(self):
        try:
            self._httpd = MyHTTPServer(('', self._config['port']), MyHTTPRequestHandler)
        except Exception as e:
            self._httpd = None
            self._logger.error(f"Caught exception while starting HTTP Server: {str(e)}")
            raise ModuleStartupError(self._my_name, "Failed to start HTTP server", str(e))
        else:
            self._httpd.on_get = self._on_get
            self._httpd.on_put = self._on_put
            self._httpd.on_post = self._on_post
            self._httpd.on_delete = self._on_delete
            self._httpd.on_options = self._on_options
            self._httpd.set_logger(self._logger)
            self._work_thread = Thread(target=self._service_loop)
            self._work_thread.start()

    def stop(self):
        if self._httpd:
            self._httpd.shutdown()
            self._logger.info(f"HTTP Server on port {self._config['port']} shut down")
        if self._work_thread:
            self._work_thread.join()

    def _service_loop(self):
        """Executes HTTP server loop and blocks on it"""
        self._logger.info(f"Starting HTTP server on port {self._config['port']}")
        self._httpd.serve_forever()

    @abstractmethod
    def _on_get(self, path: str, headers: dict, msg: str) -> RequestHandlerResult:
        """Called when a GET request received.
           Inputs: URL, request's headers, request's body.
           Outputs: Status code, response's headers, response's body.
        """
        return HTTPStatus.NOT_IMPLEMENTED, {}, ''

    @abstractmethod
    def _on_post(self, path: str, headers: dict, msg: str) -> RequestHandlerResult:
        """Called when a POST request received.
           Inputs: URL, request's headers, request's body.
           Outputs: Status code, response's headers, response's body.
        """
        return HTTPStatus.NOT_IMPLEMENTED, {}, ''

    @abstractmethod
    def _on_put(self, path: str, headers: dict, msg: str) -> RequestHandlerResult:
        """Called when a PUT request received.
           Inputs: URL, request's headers, request's body.
           Outputs: Status code, response's headers, response's body.
        """
        return HTTPStatus.NOT_IMPLEMENTED, {}, ''

    @abstractmethod
    def _on_delete(self, path: str, headers: dict) -> RequestHandlerResult:
        """Called when a DELETE request received.
           Inputs: URL, request's headers.
           Outputs: Status code, response's headers, response's body.
        """
        return HTTPStatus.NOT_IMPLEMENTED, {}, ''

    @abstractmethod
    def _on_options(self, path: str, headers: dict) -> tuple[int, dict]:
        """Called when an OPTIONS request received.
           Inputs: URL, request's headers.
           Outputs: Status code, response's headers.
        """
        return HTTPStatus.NOT_IMPLEMENTED, {}
