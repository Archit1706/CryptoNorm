"""Graceful-shutdown helper: wire SIGINT/SIGTERM to an asyncio.Event.

On POSIX (the compose/Linux target) this catches ``docker stop``'s SIGTERM
so services drain cleanly. On Windows ``add_signal_handler`` is unsupported,
so we fall back to the KeyboardInterrupt handling in each service's __main__.
"""

from __future__ import annotations

import asyncio
import signal

from cryptonorm.common.logging import get_logger


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    log = get_logger("shutdown")
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            log.debug("signal handler unsupported on this platform", signal=sig.name)
