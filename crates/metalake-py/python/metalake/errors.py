"""What can go wrong, one for one with the exit codes of the command line."""


class MetaLakeError(Exception):
    """A failure. `rule` is the protocol's rule it broke, `code` its SQLSTATE, and `retryable` whether trying again may
    work."""

    def __init__(self, message, rule=None, code=None, retryable=False):
        super().__init__(message)
        self.rule = rule
        self.code = code
        self.retryable = retryable


class UsageError(MetaLakeError, ValueError):
    """A wrong use: nothing was tried."""


class ConflictError(MetaLakeError):
    """A write that conflicts with another was refused, and nothing changed."""


class UnknownOutcomeError(MetaLakeError):
    """The connection was lost: a write that was sent may or may not have taken effect."""
