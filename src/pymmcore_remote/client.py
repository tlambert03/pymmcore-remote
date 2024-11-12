from __future__ import annotations

import os
import threading
from typing import TYPE_CHECKING, Any, ClassVar, cast

import Pyro5.api
import Pyro5.errors
from psygnal._signal import SignalInstance
from pymmcore_plus.core.events import CMMCoreSignaler
from pymmcore_plus.mda.events import MDASignaler

from . import server
from ._serialize import register_serializers

if TYPE_CHECKING:
    from psygnal import SignalInstance
    from pymmcore_plus import CMMCorePlus
    from pymmcore_plus.mda import MDARunner


class MDARunnerProxy(Pyro5.api.Proxy):
    """Proxy for MDARunner object on server."""

    def __init__(self, mda_runner_uri: Any, cb_thread: _DaemonThread) -> None:
        super().__init__(mda_runner_uri)
        events = ClientSideMDASignaler()
        object.__setattr__(self, "events", events)
        cb_thread.api_daemon.register(events)
        self.connect_client_side_callback(events)  # must come after register()

    # this is a lie... but it's more useful than -> Self
    def __enter__(self) -> MDARunner:
        """Use as a context manager."""
        return super().__enter__()  # type: ignore [no-any-return]


class MMCorePlusProxy(Pyro5.api.Proxy):
    """Proxy for CMMCorePlus object on server."""

    _mda_runner: MDARunnerProxy
    _instances: ClassVar[dict[str, MMCorePlusProxy]] = {}

    @classmethod
    def instance(cls, uri: Pyro5.api.URI | str) -> MMCorePlusProxy:
        """Return the instance for the given URI, creating it if necessary."""
        if str(uri) not in cls._instances:
            cls._instances[str(uri)] = cls(uri)
        return cls._instances[str(uri)]

    def __init__(
        self, uri: Pyro5.api.URI | str | None = None, connected_socket: Any = None
    ) -> None:
        if uri is None:
            uri = f"PYRO:{server.CORE_NAME}@{server.DEFAULT_HOST}:{server.DEFAULT_PORT}"
        register_serializers()
        super().__init__(uri, connected_socket=connected_socket)
        self._instances[str(self._pyroUri)] = self

        # check that the connection is valid
        try:
            self._pyroBind()
        except Pyro5.errors.CommunicationError as e:
            raise ConnectionRefusedError(
                f"Failed to connect to server at {uri}.\n"
                "Is the pymmcore-plus server running? "
                "You can start it with: 'mmcore-remote'"
            ) from e

        # create a proxy object to receive and connect CMMCoreSignaler events
        # here on the client side
        events = ClientSideCMMCoreSignaler(self)
        object.__setattr__(self, "events", events)
        # create daemon thread to listen for callbacks/signals coming from the server
        # and register the callback handler
        cb_thread = _DaemonThread(name="CallbackDaemon")
        cb_thread.api_daemon.register(events)
        # connect our local callback handler to the server's signaler
        self.connect_client_side_callback(events)  # must come after register()

        # Create a proxy object for the mda_runner as well, passing in the daemon thread
        # so it too can receive signals from the server
        object.__setattr__(
            self, "_mda_runner", MDARunnerProxy(self.get_mda_runner_uri(), cb_thread)
        )
        # start the callback-handling thread
        cb_thread.start()

    @property
    def mda(self) -> MDARunner:
        """Return the MDARunner proxy."""
        return self._mda_runner

    # this is a lie... but it's more useful than -> Self
    def __enter__(self) -> CMMCorePlus:
        """Use as a context manager."""
        return super().__enter__()  # type: ignore [no-any-return]


@Pyro5.api.expose  # type: ignore [misc]
def receive_server_callback(self: Any, signal_name: str, args: tuple) -> None:
    """Will be called by server with name of signal, and tuple of args."""
    print(
        "Received callback from server:",
        signal_name,
        args,
        threading.current_thread(),
        "pid",
        os.getpid(),
    )
    signal = cast("SignalInstance", getattr(self, signal_name))
    print("  Emitting signal:", signal)
    signal.emit(*args)
    print("  Signal emitted")


def _patched_getattr(self: Any, __name: str) -> Any:
    attr = object.__getattribute__(self, __name)
    if hasattr(attr, "connect"):
        func = attr.connect

        def _patched(*args: Any, **kwargs: Any) -> Any:
            kwargs["thread"] = "main"
            return func(*args, **kwargs)

        attr.connect = _patched
    return attr


class ClientSideCMMCoreSignaler(CMMCoreSignaler):
    """Client-side signaler for CMMCore events."""

    def __init__(self, proxy: Any = None) -> None:
        self._proxy = proxy
        super().__init__()

    receive_server_callback = receive_server_callback

    # TOTAL HACK
    __getattribute__ = _patched_getattr


class ClientSideMDASignaler(MDASignaler):
    """Client-side signaler for MDA events."""

    receive_server_callback = receive_server_callback
    # TOTAL HACK
    __getattribute__ = _patched_getattr


class _DaemonThread(threading.Thread):
    def __init__(self, name: str = "DaemonThread"):
        self.api_daemon = Pyro5.api.Daemon()
        self._stop_event = threading.Event()
        super().__init__(target=self.api_daemon.requestLoop, name=name, daemon=True)
