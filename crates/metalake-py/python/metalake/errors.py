"""What can go wrong, one for one with the exit codes of the command line."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._metalake import Done as _Done


class MetaLakeError(Exception):
    """A failure. `rule` is the protocol's rule it broke, `code` its SQLSTATE, and `retryable` whether trying again may
    work. From `maintain`, `done` is what had been done."""

    def __init__(self, message: str, rule: str | None = None, code: str | None = None, retryable: bool = False):
        super().__init__(message)
        self.rule = rule
        self.code = code
        self.retryable = retryable
        self.done: list[_Done] = []

    def __reduce__(self):
        # Across processes, as `multiprocessing` sends it. What `maintain` had done stays behind.
        return (type(self), (self.args[0], self.rule, self.code, self.retryable))


class UsageError(MetaLakeError, ValueError):
    """A wrong use: nothing was tried."""


class ConflictError(MetaLakeError):
    """A write that conflicts with another was refused, and nothing changed."""


class UnknownOutcomeError(MetaLakeError):
    """The connection was lost: a write that was sent may or may not have taken effect."""
