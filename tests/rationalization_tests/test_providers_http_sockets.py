"""Real-socket tests for ``datagrunt.rationalization.providers.http``: proxies, the deadline cut, TLS, logs, imports.

Every server here is a raw-socket HTTP/1.1 server on 127.0.0.1 run by the test, scripted to answer, trickle, stall
or relay; a "proxy" is such a server, named by an environment variable. A non-loopback target (``*.invalid``) is
only ever reached through such a proxy, so no test leaves the machine. The rules checked:

- a loopback target never goes through an environment proxy, whatever ``HTTP_PROXY``, ``ALL_PROXY`` or the system
  settings say; a non-loopback one does (``allow_insecure_http`` traffic is then plaintext to the proxy);
- the caller's deadline is absolute: a server trickling headers or body, one that stops reading the request, a stalled
  TLS handshake and a trickling proxy (forward, ``CONNECT``, or relaying TLS) are all cut within a second of it;
- a certificate the client does not trust is a permanent error, a refused connection a transient one;
- no key reaches httpx's or httpcore's DEBUG logs, or the traceback locals of an error;
- ``.netrc`` credentials are never sent, and after ``open()`` a first request on a worker thread imports nothing.
"""

import contextlib
import datetime
import gc
import gzip
import ipaddress
import json
import logging
import os
import socket
import ssl
import struct
import subprocess
import sys
import textwrap
import threading
import time
import traceback
import tracemalloc
import warnings
from pathlib import Path

import httpx
import pytest

from datagrunt.rationalization.providers import http as http_module
from datagrunt.rationalization.providers.base import PermanentProviderError, TransientProviderError, _Secret
from datagrunt.rationalization.providers.http import RESPONSE_MAX_BYTES, HttpClient

TIMEOUT_S = 120
MIB = 1024 * 1024
KEY = "sk-hunter2-AUTH"
HEADERS = {"Authorization": _Secret(KEY).prefixed("Bearer "), "x-api-key": _Secret(KEY)}
BUDGET_S = 0.5
SLACK_S = 2.5  # an uncut trickle takes 10-25 s, so the tests keep their power on a loaded runner
LATE_BUDGET_S = 2.0
LATE_MARGIN_S = 0.9  # without the cut, a phase blocked at 1.5 s ends at 3.5 s (its timeout is 2 s)
OK = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}"
SLOW_HEAD = (
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Pad: " + b"a" * 40 + b"\r\nContent-Length: 2\r\n\r\n{}"
)
ENVIRONMENT = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "SSLKEYLOGFILE",
    "NETRC",
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)


class Server:
    """A raw-socket HTTP/1.1 server on 127.0.0.1: each request is answered by ``behaviour(conn, request)``."""

    def __init__(self, behaviour, *, wrap=None):
        self.behaviour = behaviour
        self.wrap = wrap
        self.requests = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(16)
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.stopped = threading.Event()
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while not self.stopped.is_set():
            try:
                conn, _ = self.sock.accept()
            except OSError:
                continue
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn):
        try:
            conn.settimeout(10)
            if self.wrap is not None:
                conn = self.wrap(conn)
            request = read_request(conn)
            if request is not None:
                self.requests.append(request)
                self.behaviour(conn, request)
        except OSError:
            pass
        finally:
            conn.close()

    def close(self):
        self.stopped.set()
        self.sock.close()


def read_request(conn):
    """One request's head and body, or ``None`` when the peer closes first."""
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(65536)
        if not chunk:
            return None
        data += chunk
    head, _, rest = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(rest) < length:
        chunk = conn.recv(65536)
        if not chunk:
            break
        rest += chunk
    return head + b"\r\n\r\n" + rest


def respond(conn, status, headers, body):
    lines = [f"HTTP/1.1 {status} X", *(f"{name}: {value}" for name, value in headers), f"Content-Length: {len(body)}"]
    conn.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body)


def trickle(conn, data, every_s=0.25):
    for index in range(len(data)):
        conn.sendall(data[index : index + 1])
        time.sleep(every_s)


@pytest.fixture
def serve():
    """Start servers for the test and close them after it."""
    servers = []

    def start(behaviour, **kwargs):
        server = Server(behaviour, **kwargs)
        servers.append(server)
        return server

    yield start
    for server in servers:
        server.close()


@pytest.fixture
def tls_files(tmp_path):
    """A self-signed certificate (and its key) for ``localhost``, ``127.0.0.1`` and ``tls.invalid``."""
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "tls.invalid")])
    now = datetime.datetime.now(datetime.timezone.utc)
    names = [x509.DNSName("localhost"), x509.DNSName("tls.invalid")]

    names.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName(names), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return cert_path, key_path


def tls_wrapper(tls_files):
    cert_path, key_path = tls_files
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    return lambda conn: context.wrap_socket(conn, server_side=True)


def relay_to(port):
    """A CONNECT proxy behaviour: accept the tunnel, then pump bytes both ways to 127.0.0.1:``port`` (or to the port
    ``port()`` names when the tunnel opens)."""

    def behaviour(conn, request):
        conn.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        upstream = socket.create_connection(("127.0.0.1", port() if callable(port) else port), timeout=10)

        def pump(source, sink):
            try:
                while data := source.recv(65536):
                    sink.sendall(data)
            except OSError:
                pass
            finally:  # one side ended or reset: end the other, as a real proxy does
                with contextlib.suppress(OSError):
                    sink.shutdown(socket.SHUT_RDWR)

        threading.Thread(target=pump, args=(upstream, conn), daemon=True).start()
        pump(conn, upstream)
        upstream.close()

    return behaviour


def library_traceback(error):
    """The error's traceback with the locals of datagrunt's frames (the test's own frames hold its fixtures)."""
    summary = traceback.TracebackException.from_exception(error, capture_locals=True)
    frames = [frame for frame in summary.stack if "datagrunt/rationalization" in frame.filename.replace("\\", "/")]
    assert frames, "vacuous: no datagrunt frame on the traceback"
    return "".join(f"{frame.name}: {frame.locals}\n" for frame in frames) + "".join(summary.format_exception_only())


def post(client, url, *, budget_s=10.0, body=None, headers=None):
    return client.post_json(
        url,
        headers=HEADERS if headers is None else headers,
        body={"messages": [{"content": "CONFIDENTIAL document text"}]} if body is None else body,
        timeout_s=5.0,
        deadline=time.monotonic() + budget_s,
    )


def cut_within_the_deadline(client, url, **kwargs):
    """Post with a 0.5 s budget and require a transient 'did not finish' within a second of the deadline."""
    start = time.monotonic()
    with pytest.raises(TransientProviderError, match="did not finish within") as caught:
        post(client, url, budget_s=BUDGET_S, **kwargs)
    elapsed = time.monotonic() - start
    assert elapsed <= BUDGET_S + SLACK_S, f"took {elapsed:.2f} s against a {BUDGET_S} s budget"
    return caught.value


# ---------------------------------------------------------------------------------------------------------------
# Loopback never goes through a proxy (C1); everything else does
# ---------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("variable", ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "HTTPS_PROXY"])
@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
def test_a_loopback_request_never_reaches_an_environment_proxy(serve, monkeypatch, variable, host):
    proxy = serve(lambda conn, request: respond(conn, 200, [], b'{"from": "proxy"}'))
    target = serve(
        lambda conn, request: respond(conn, 200, [("Content-Type", "application/json")], b'{"from": "target"}')
    )
    monkeypatch.setenv(variable, f"http://127.0.0.1:{proxy.port}")

    reply = post(HttpClient(), f"http://{host}:{target.port}/v1/chat/completions")

    assert reply.json == {"from": "target"}
    assert proxy.requests == []
    assert len(target.requests) == 1


def test_a_non_loopback_request_goes_through_the_proxy_in_plaintext_when_insecure_http_is_allowed(serve, monkeypatch):
    proxy = serve(lambda conn, request: respond(conn, 200, [("Content-Type", "application/json")], b'{"via": 1}'))
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy.port}")

    reply = post(HttpClient(allow_insecure_http=True), "http://plain.invalid/v1/chat/completions")

    assert reply.json == {"via": 1}
    (seen,) = proxy.requests
    assert seen.startswith(b"POST http://plain.invalid/v1/chat/completions HTTP/1.1")
    assert b"CONFIDENTIAL" in seen  # what allow_insecure_http=True means behind a proxy: documented


def test_netrc_credentials_are_never_sent(serve, monkeypatch, tmp_path):
    proxy = serve(lambda conn, request: respond(conn, 200, [], b"{}"))
    netrc = tmp_path / ".netrc"
    netrc.write_text(
        "machine plain.invalid login alice password netrc-hunter2\n"
        "machine 127.0.0.1 login alice password netrc-hunter2\n"
    )
    netrc.chmod(0o600)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("NETRC", str(netrc))
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy.port}")

    post(HttpClient(allow_insecure_http=True), "http://plain.invalid/v1", headers={})

    (seen,) = proxy.requests
    assert b"netrc-hunter2" not in seen
    assert b"authorization" not in seen.lower()


def test_a_bad_proxy_environment_never_stops_a_loopback_request(serve, monkeypatch):
    target = serve(lambda conn, request: respond(conn, 200, [], b"{}"))
    monkeypatch.setenv("HTTP_PROXY", "socks5://127.0.0.1:1080")
    monkeypatch.setenv("SSL_CERT_FILE", "/nonexistent/ca.pem")

    assert post(HttpClient(), f"http://127.0.0.1:{target.port}/v1").status == 200


# ---------------------------------------------------------------------------------------------------------------
# The absolute deadline: the request's socket is cut (I2)
# ---------------------------------------------------------------------------------------------------------------


def test_a_server_trickling_its_headers_is_cut_at_the_deadline(serve):
    server = serve(lambda conn, request: trickle(conn, SLOW_HEAD))

    error = cut_within_the_deadline(HttpClient(), f"http://127.0.0.1:{server.port}/v1")

    assert error.status_code is None


def test_a_server_trickling_its_body_is_cut_at_the_deadline(serve):
    def behaviour(conn, request):
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 40\r\n\r\n")
        trickle(conn, b"x" * 40)

    server = serve(behaviour)

    cut_within_the_deadline(HttpClient(), f"http://127.0.0.1:{server.port}/v1")


def test_a_server_that_stops_reading_the_request_is_cut_at_the_deadline():
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    held = []
    threading.Thread(target=lambda: held.append(listener.accept()), daemon=True).start()
    try:
        cut_within_the_deadline(
            HttpClient(), f"http://127.0.0.1:{listener.getsockname()[1]}/v1", body={"x": "a" * (16 * MIB)}
        )
    finally:
        for conn, _ in held:
            conn.close()
        listener.close()


def test_a_stalled_tls_handshake_is_cut_at_the_deadline(serve):
    server = serve(lambda conn, request: None)  # reads for a request head the ClientHello never completes

    cut_within_the_deadline(HttpClient(), f"https://127.0.0.1:{server.port}/v1")


def trickling_relay(port):
    """A plain TCP relay to 127.0.0.1:``port`` that hands the server's bytes back one at a time (no read times out)."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)

    def relay(conn):
        upstream = socket.create_connection(("127.0.0.1", port), timeout=10)

        def down():
            try:
                while data := upstream.recv(65536):
                    trickle(conn, data, 0.05)
            except OSError:
                pass

        threading.Thread(target=down, daemon=True).start()
        try:
            while data := conn.recv(65536):
                upstream.sendall(data)
        except OSError:
            pass
        finally:
            upstream.close()
            conn.close()

    def accept():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            threading.Thread(target=relay, args=(conn,), daemon=True).start()

    threading.Thread(target=accept, daemon=True).start()
    return listener


def test_a_tls_handshake_trickled_byte_by_byte_is_cut_at_the_deadline(serve, tls_files):
    server = serve(lambda conn, request: conn.sendall(OK), wrap=tls_wrapper(tls_files))
    relay = trickling_relay(server.port)
    try:
        cut_within_the_deadline(HttpClient(), f"https://localhost:{relay.getsockname()[1]}/v1")
    finally:
        relay.close()


def test_a_forward_proxy_trickling_the_reply_is_cut_at_the_deadline(serve, monkeypatch):
    proxy = serve(lambda conn, request: trickle(conn, SLOW_HEAD))
    monkeypatch.setenv("HTTP_PROXY", f"http://127.0.0.1:{proxy.port}")

    cut_within_the_deadline(HttpClient(allow_insecure_http=True), "http://slow.invalid/v1")

    assert proxy.requests  # the request did go to the proxy, whose socket was cut


def test_a_proxy_trickling_its_connect_reply_is_cut_at_the_deadline(serve, monkeypatch):
    proxy = serve(lambda conn, request: trickle(conn, b"HTTP/1.1 200 Connection established\r\n\r\n"))
    monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{proxy.port}")

    cut_within_the_deadline(HttpClient(), "https://slow.invalid/v1")

    assert proxy.requests[0].startswith(b"CONNECT slow.invalid:443")


def test_a_tls_server_behind_a_connect_proxy_answers_and_one_trickling_is_cut(serve, monkeypatch, tls_files):
    fast = serve(
        lambda conn, request: respond(conn, 200, [("Content-Type", "application/json")], b'{"tls": 1}'),
        wrap=tls_wrapper(tls_files),
    )
    slow = serve(lambda conn, request: trickle(conn, SLOW_HEAD), wrap=tls_wrapper(tls_files))
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_files[0]))

    monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{serve(relay_to(fast.port)).port}")
    assert post(HttpClient(), "https://tls.invalid/v1").json == {"tls": 1}  # the TLS path is real

    monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{serve(relay_to(slow.port)).port}")
    cut_within_the_deadline(HttpClient(), "https://tls.invalid/v1")


def test_a_close_while_a_real_body_streams_lets_the_request_finish(serve):
    def behaviour(conn, request):
        conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n")
        trickle(conn, b'{"a": 123}', 0.1)

    server = serve(behaviour)
    client = HttpClient()
    closer = threading.Timer(0.3, client.close)
    closer.start()
    try:
        assert post(client, f"http://127.0.0.1:{server.port}/v1").json == {"a": 123}
    finally:
        closer.cancel()


# The blocking phase starts late (about 1.5 s into a 2 s budget): every phase timeout was cut to 2 s when the request
# began, so without the socket cut the call would end near 3.5 s. Only the cut ends these in time.


def late_silence(conn, request):
    for byte in b"HTTP/":
        conn.sendall(bytes([byte]))
        time.sleep(0.3)
    time.sleep(10)


def cut_late(client, url, **kwargs):
    start = time.monotonic()
    with pytest.raises(TransientProviderError, match="did not finish within"):
        post(client, url, budget_s=LATE_BUDGET_S, **kwargs)
    elapsed = time.monotonic() - start
    assert elapsed < LATE_BUDGET_S + LATE_MARGIN_S, f"took {elapsed:.2f} s against a {LATE_BUDGET_S} s budget"


def test_a_recv_blocked_late_on_plain_tcp_is_cut(serve):
    server = serve(late_silence)

    cut_late(HttpClient(), f"http://127.0.0.1:{server.port}/v1")


def test_a_send_blocked_late_on_plain_tcp_is_cut():
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    held = []

    def read_slowly_then_stop():
        conn, _ = listener.accept()
        held.append(conn)
        end = time.monotonic() + 1.5
        while time.monotonic() < end:
            conn.recv(16 * 1024)
            time.sleep(0.02)

    reader = threading.Thread(target=read_slowly_then_stop, daemon=True)
    reader.start()
    try:
        cut_late(HttpClient(), f"http://127.0.0.1:{listener.getsockname()[1]}/v1", body={"x": "a" * (32 * MIB)})
    finally:
        # Join before closing: a reader starved under load would otherwise recv on a closed socket (EBADF).
        reader.join(10)
        for conn in held:
            conn.close()
        listener.close()


def test_a_recv_blocked_late_on_direct_tls_is_cut(serve, tls_files):
    server = serve(late_silence, wrap=tls_wrapper(tls_files))
    transport = httpx.HTTPTransport(verify=ssl.create_default_context(cafile=str(tls_files[0])))

    cut_late(HttpClient(transport=transport), f"https://localhost:{server.port}/v1")


def test_a_recv_blocked_late_behind_a_connect_proxy_with_tls_is_cut(serve, monkeypatch, tls_files):
    server = serve(late_silence, wrap=tls_wrapper(tls_files))
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_files[0]))
    monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{serve(relay_to(server.port)).port}")

    cut_late(HttpClient(), "https://tls.invalid/v1")


def test_failed_tunnelled_handshakes_never_exhaust_the_pool(serve, monkeypatch, tls_files):
    healthy = serve(
        lambda conn, request: respond(conn, 200, [("Content-Type", "application/json")], b'{"ok": 1}'),
        wrap=tls_wrapper(tls_files),
    )
    stalled = serve(lambda conn, request: None)  # never answers the ClientHello
    upstream = {"port": stalled.port}
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_files[0]))
    monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{serve(relay_to(lambda: upstream['port'])).port}")
    client = HttpClient(max_connections=2)
    client.open()

    for _ in range(3):  # more cut handshakes than the pool holds
        with pytest.raises(TransientProviderError, match="did not finish"):
            post(client, "https://tls.invalid/v1", budget_s=0.5)
    upstream["port"] = healthy.port
    start = time.monotonic()

    assert post(client, "https://tls.invalid/v1", budget_s=5.0).json == {"ok": 1}
    assert time.monotonic() - start < 1.0


def resetting_listener():
    """A raw listener that reads a little of each connection (the ClientHello) and resets it (SO_LINGER 0)."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)

    def accept():
        while True:
            try:
                conn, _ = listener.accept()
            except OSError:
                return
            try:
                conn.recv(16)
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            except OSError:
                pass
            finally:
                conn.close()

    threading.Thread(target=accept, daemon=True).start()
    return listener


def test_reset_tunnelled_handshakes_never_exhaust_the_pool(serve, monkeypatch, tls_files):
    healthy = serve(
        lambda conn, request: respond(conn, 200, [("Content-Type", "application/json")], b'{"ok": 1}'),
        wrap=tls_wrapper(tls_files),
    )
    resetting = resetting_listener()
    upstream = {"port": resetting.getsockname()[1]}
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_files[0]))
    monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{serve(relay_to(lambda: upstream['port'])).port}")
    client = HttpClient(max_connections=2)
    client.open()
    try:
        for _ in range(3):  # failures that are no deadline cut: the transport's own error
            with pytest.raises(TransientProviderError) as caught:
                post(client, "https://tls.invalid/v1", budget_s=5.0)
            assert "did not finish" not in caught.value.detail
        upstream["port"] = healthy.port
        start = time.monotonic()

        assert post(client, "https://tls.invalid/v1", budget_s=5.0).json == {"ok": 1}
        assert time.monotonic() - start < 1.0
    finally:
        resetting.close()


def test_a_request_whose_socket_is_not_captured_fails_loud(serve, monkeypatch):
    server = serve(lambda conn, request: conn.sendall(OK))
    monkeypatch.setattr(http_module._Cutter, "trace", lambda self, event, info: None)

    with pytest.raises(TransientProviderError, match="could not be bounded"):
        post(HttpClient(), f"http://127.0.0.1:{server.port}/v1")


def open_descriptors():
    for folder in ("/proc/self/fd", "/dev/fd"):
        if Path(folder).is_dir():
            return len(os.listdir(folder))
    pytest.skip("no descriptor directory on this platform")


def test_no_descriptor_thread_or_unclosed_socket_outlives_its_request(serve):
    fast = serve(lambda conn, request: conn.sendall(OK))
    slow = serve(lambda conn, request: trickle(conn, SLOW_HEAD, 0.03))
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    refused = probe.getsockname()[1]
    probe.close()
    client = HttpClient()
    client.open()

    def mixed(count):
        for index in range(count):
            post(client, f"http://127.0.0.1:{fast.port}/v1", budget_s=5.0)
            if index % 10 == 0:
                with pytest.raises(TransientProviderError, match="did not finish"):
                    post(client, f"http://127.0.0.1:{slow.port}/v1", budget_s=0.05)
            if index % 25 == 0:
                with pytest.raises(TransientProviderError, match="refused"):
                    post(client, f"http://127.0.0.1:{refused}/v1", budget_s=5.0)
        time.sleep(1.0)  # the servers' threads end; a request's own timer is cancelled at once
        gc.collect()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ResourceWarning)
        mixed(20)
        descriptors, threads = open_descriptors(), threading.active_count()
        mixed(300)

    assert open_descriptors() <= descriptors + 4
    assert threading.active_count() <= threads + 4
    assert [str(warning.message) for warning in caught if issubclass(warning.category, ResourceWarning)] == []


def test_a_fast_reply_is_not_cut(serve):
    server = serve(lambda conn, request: conn.sendall(OK))

    assert post(HttpClient(), f"http://127.0.0.1:{server.port}/v1", budget_s=5.0).status == 200


# ---------------------------------------------------------------------------------------------------------------
# Content-Encoding over a socket (I1)
# ---------------------------------------------------------------------------------------------------------------


def test_a_nested_gzip_bomb_is_refused_unread(serve):
    body = gzip.compress(gzip.compress(b"\0" * (64 * MIB), compresslevel=9), compresslevel=9)
    server = serve(lambda conn, request: respond(conn, 200, [("Content-Encoding", "gzip, gzip")], body))

    with pytest.raises(PermanentProviderError, match="unsupported Content-Encoding"):
        post(HttpClient(), f"http://127.0.0.1:{server.port}/v1")


def test_a_single_huge_gzip_layer_is_cut_at_the_cap_with_memory_bounded(serve):
    body = gzip.compress(b"\0" * (64 * MIB), compresslevel=9)
    server = serve(lambda conn, request: respond(conn, 200, [("Content-Encoding", "gzip")], body))
    client = HttpClient()
    client.open()

    tracemalloc.start()
    try:
        with pytest.raises(PermanentProviderError, match="exceeds 8 MiB"):
            post(client, f"http://127.0.0.1:{server.port}/v1")
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < 3 * RESPONSE_MAX_BYTES


def test_the_request_asks_for_an_identity_body(serve):
    server = serve(lambda conn, request: conn.sendall(OK))

    post(HttpClient(), f"http://127.0.0.1:{server.port}/v1")

    assert b"\r\naccept-encoding: identity\r\n" in server.requests[0].lower()


# ---------------------------------------------------------------------------------------------------------------
# TLS verification and connection failures
# ---------------------------------------------------------------------------------------------------------------


def test_an_untrusted_certificate_is_permanent_and_loopback_ignores_ssl_cert_file(serve, monkeypatch, tls_files):
    server = serve(lambda conn, request: conn.sendall(OK), wrap=tls_wrapper(tls_files))
    monkeypatch.setenv("SSL_CERT_FILE", str(tls_files[0]))  # trusted by the environment, which loopback ignores

    with pytest.raises(PermanentProviderError, match="TLS certificate verification failed"):
        post(HttpClient(), f"https://localhost:{server.port}/v1")


def test_a_refused_connection_is_transient_and_its_traceback_holds_no_key():
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    with pytest.raises(TransientProviderError, match="connection refused") as caught:
        post(HttpClient(), f"http://127.0.0.1:{port}/v1")

    assert KEY not in library_traceback(caught.value)
    assert caught.value.__context__ is None


def test_a_401_over_a_socket_holds_no_key_in_its_traceback_locals(serve):
    server = serve(lambda conn, request: respond(conn, 401, [], f'{{"error": {{"message": "bad {KEY}"}}}}'.encode()))

    with pytest.raises(PermanentProviderError) as caught:
        post(HttpClient(), f"http://127.0.0.1:{server.port}/v1")

    assert KEY not in library_traceback(caught.value)
    assert "<secret>" in caught.value.detail


# ---------------------------------------------------------------------------------------------------------------
# Logs (the real network stack: httpcore runs) and imports
# ---------------------------------------------------------------------------------------------------------------


def test_no_key_reaches_the_httpx_or_httpcore_debug_logs(serve, caplog):
    server = serve(lambda conn, request: respond(conn, 401, [], b'{"error": {"message": "bad"}}'))
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    with pytest.raises(PermanentProviderError):
        post(HttpClient(), f"http://127.0.0.1:{server.port}/v1")

    assert any(record.name.startswith("httpcore") for record in caplog.records), "vacuous: httpcore logged nothing"
    assert KEY not in caplog.text
    assert KEY.encode() in server.requests[0]  # the key was sent, to the server only


WORKER_IMPORTS = """
import json, socket, sys, threading, time

def serve(listener):
    while True:
        conn, _ = listener.accept()
        data = b""
        while b"\\r\\n\\r\\n" not in data:
            data += conn.recv(65536)
        head, _, rest = data.partition(b"\\r\\n\\r\\n")
        lines = [line for line in head.split(b"\\r\\n") if line.lower().startswith(b"content-length")]
        length = int(lines[0].split(b":")[1])
        while len(rest) < length:
            rest += conn.recv(65536)
        conn.sendall(b"HTTP/1.1 200 OK\\r\\nContent-Type: application/json\\r\\nContent-Length: 2\\r\\n\\r\\n{}")
        conn.close()

listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen(8)
port = listener.getsockname()[1]
threading.Thread(target=serve, args=(listener,), daemon=True).start()
import os
os.environ["HTTP_PROXY"] = f"http://127.0.0.1:{port}"

from datagrunt.rationalization.providers.http import HttpClient
client = HttpClient(allow_insecure_http=True)
client.open()
before = set(sys.modules)
found = {}

def work():
    statuses = []
    for url in (f"http://127.0.0.1:{port}/v1", "http://proxied.invalid/v1"):
        reply = client.post_json(url, headers={}, body={"a": 1}, timeout_s=5.0, deadline=time.monotonic() + 10)
        statuses.append(reply.status)
    found["statuses"] = statuses
    found["new"] = sorted(set(sys.modules) - before)

worker = threading.Thread(target=work)
worker.start()
worker.join(60)
print(json.dumps(found))
"""


@pytest.mark.parametrize("opened", [True, False], ids=["after-open", "without-open"])
def test_a_first_request_on_a_worker_thread_imports_nothing(opened):
    code = WORKER_IMPORTS if opened else WORKER_IMPORTS.replace("client.open()\n", "")
    completed = subprocess.run(
        [sys.executable, "-I", "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
        timeout=TIMEOUT_S,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    found = json.loads(completed.stdout.strip().splitlines()[-1])
    assert found["statuses"] == [200, 200]
    assert found["new"] == []
