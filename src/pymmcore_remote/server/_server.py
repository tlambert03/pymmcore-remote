from __future__ import annotations

import contextlib
import os
import subprocess
import sys
import threading
import time
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Protocol, cast

import Pyro5
import Pyro5.api
import Pyro5.core
import Pyro5.errors
from pymmcore_plus import CMMCorePlus
from pymmcore_plus.core.events import CMMCoreSignaler
from pymmcore_plus.mda import MDAEngine, MDARunner
from pymmcore_plus.mda.events import MDASignaler

from pymmcore_remote._serialize import register_serializers
from pymmcore_remote._util import wrap_for_pyro

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from click import Path
    from useq import MDAEvent

with contextlib.suppress(ImportError):
    from rich import print

MDA_RUNNER_NAME = "pymmcore.mda.MDARunner"
CORE_NAME = "pymmcore.CMMCorePlus"
DEFAULT_PORT = 54333
DEFAULT_HOST = "127.0.0.1"
DEFAULT_URI = f"PYRO:{CORE_NAME}@{DEFAULT_HOST}:{DEFAULT_PORT}"
GLOBAL_DAEMON: Pyro5.api.Daemon | None = None


class ClientSideCallbackHandler(Protocol):
    """Protocol for callback handlers on the client side."""

    def receive_server_callback(self, signal_name: str, args: tuple) -> None:
        """Will be called by server with name of signal, and tuple of args."""


class _CallbackMixin:
    def __init__(self, signal_type: type, events: Any) -> None:
        self._callback_handlers: set[ClientSideCallbackHandler] = set()

        for name in {
            name
            for name in dir(signal_type)
            if not name.startswith("_") and name != "all"
        }:
            attr = getattr(events, name)
            if hasattr(attr, "connect"):
                # FIXME: devicePropertyChanged will not work on Remote
                attr.connect(partial(self.emit_signal, name))

    def connect_client_side_callback(self, handler: ClientSideCallbackHandler) -> None:
        self._callback_handlers.add(handler)

    def disconnect_client_side_callback(
        self, handler: ClientSideCallbackHandler
    ) -> None:
        self._callback_handlers.discard(handler)

    @Pyro5.api.oneway  # type: ignore [misc]
    def emit_signal(self, signal_name: str, *args: Any) -> None:
        for handler in list(self._callback_handlers):
            try:
                handler._pyroClaimOwnership()  # type: ignore
                print(
                    "Emitting server signal",
                    threading.current_thread(),
                    "pid",
                    os.getpid(),
                )
                handler.receive_server_callback(signal_name, args)
            except Pyro5.errors.CommunicationError:  # pragma: no cover
                self._callback_handlers.discard(handler)


@Pyro5.api.expose
@Pyro5.api.behavior(instance_mode="single")
@wrap_for_pyro
class RemoteCMMCorePlus(CMMCorePlus, _CallbackMixin):
    """CMMCorePlus with Pyro5 serialization, running on a remote process."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        CMMCorePlus.__init__(self, *args, **kwargs)
        _CallbackMixin.__init__(self, CMMCoreSignaler, self.events)
        self._mda_runner = RemoteMDARunner(self)
        if GLOBAL_DAEMON:
            self._mda_runner_uri = GLOBAL_DAEMON.register(self._mda_runner)

    def ping(self) -> str:
        """A simple do-nothing method for testing purposes."""
        return "pong"

    def get_mda_runner_uri(self) -> Pyro5.core.URI:
        """Return the URI of the remote MDARunner instance."""
        return self._mda_runner_uri

    def run_mda(  # type: ignore [override]
        self,
        events: Iterable[MDAEvent],
        *,
        output: Path | str | object | Sequence[Path | str | object] | None = None,
        block: bool = False,
    ) -> None:
        """Run an MDA sequence in another thread on the server side."""
        # overriding to return None, so as not to serialize the thread object
        super().run_mda(events, output=output, block=block)


@Pyro5.api.expose
@wrap_for_pyro  # TODO: not sure why this would be needed on non SWIG classes
class RemoteMDARunner(MDARunner, _CallbackMixin):
    """MDARunner with Pyro5 serialization, running on a remote process."""

    def __init__(self, core: CMMCorePlus) -> None:
        MDARunner.__init__(self)
        _CallbackMixin.__init__(self, MDASignaler, self.events)
        self._engine = MDAEngine(core)


def _print(msg: str, color: str = "", bold: bool = False, end: str = "\n") -> None:
    if print.__module__ == "rich":
        msg = f"[{color}]{msg}[/{color}]"
        if bold:
            msg = f"[bold]{msg}[/bold]"
    print(msg, end=end)


def _logger_or_print(level: str = "info") -> Callable[[str], None]:
    """Get a logger or print function based on the availability of the logger."""
    try:
        from pymmcore_plus._logger import logger

        return getattr(logger, level)  # type: ignore
    except ImportError:
        return print


def serve(
    host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, verbose: bool = False
) -> None:
    """Start a blocking Pyro5 server with a CMMCorePlus instance."""
    global GLOBAL_DAEMON

    register_serializers()
    log = _logger_or_print("info")

    objects: dict[type, str] = {RemoteCMMCorePlus: CORE_NAME}
    with (GLOBAL_DAEMON := Pyro5.api.Daemon(host=host, port=port)):
        for obj, name in objects.items():
            uri = GLOBAL_DAEMON.register(obj, name)
            if verbose:
                log(f"Registered object {obj!r}:\n    uri = {uri}")
        if verbose:
            from pymmcore_plus import configure_logging

            configure_logging(stderr_level="DEBUG")
            Pyro5.config.DETAILED_TRACEBACK = True

        log(f"pymmcore-remote daemon listening at {host}:{port}. [Ctrl+C to exit]")
        GLOBAL_DAEMON.requestLoop()


@contextlib.contextmanager
def server_process(
    host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, timeout: float = 3.0
) -> Iterator[subprocess.Popen | None]:
    """Context manager for starting a Pyro5 server in a separate process.

    If the server is already running, the context manager will yield None, and will
    not clean up the server process when done.
    """
    log = _logger_or_print("info")

    uri = f"PYRO:{Pyro5.core.DAEMON_NAME}@{host}:{port}"
    remote_daemon = cast(Pyro5.api.DaemonObject, Pyro5.api.Proxy(uri))
    with contextlib.suppress(Pyro5.errors.CommunicationError):
        remote_daemon.ping()
        # if we get here, the server is already running
        log(f"Server process at {host}:{port} already running.")
        yield None
        return

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "pymmcore_remote.server",
            "--host",
            host,
            "--port",
            str(port),
        ]
    )

    while timeout > 0:
        try:
            remote_daemon.ping()
            break
        except Exception:
            timeout -= 0.1
            time.sleep(0.1)
    else:
        raise TimeoutError(f"Could not connect to server at {host}:{port}")

    yield proc
    proc.kill()
    proc.wait()
    log(f"Server process at {host}:{port} terminated.")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT, help="port")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("-v", "--verbose", action="store_true", default=False)
    args = parser.parse_args()

    serve(host=args.host, port=args.port, verbose=args.verbose)
