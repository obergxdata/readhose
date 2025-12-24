import os
import pytest
import threading
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler


class QuietHTTPRequestHandler(SimpleHTTPRequestHandler):
    """HTTP request handler that doesn't log to stderr."""
    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="session", autouse=True)
def test_http_server():
    """Start a test HTTP server for the duration of the entire test session."""
    # Point to the HTTP test HTML files
    html_dir = Path(__file__).parent / "fetch" / "HTTP" / "tests" / "html"

    original_dir = os.getcwd()
    os.chdir(html_dir)

    server = HTTPServer(('localhost', 8765), QuietHTTPRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield server

    server.shutdown()
    os.chdir(original_dir)
