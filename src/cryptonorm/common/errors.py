"""Shared exception types."""

from __future__ import annotations


class FeedError(Exception):
    """Base for recoverable feed problems (the reconnect wrapper retries)."""


class FeedGapError(FeedError):
    """An order-book desync was detected (sequence gap or checksum mismatch).

    Adapters raise this to force a resync: the reconnect wrapper drops the
    connection and reconnects, which re-snapshots the book.
    """
