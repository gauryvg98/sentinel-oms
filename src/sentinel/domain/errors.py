"""Domain errors.

The distinction between IllegalTransition and RequiresReconciliation is
load-bearing: the first means the caller has a bug; the second means the
outside world produced evidence we can't apply directly (a fill for an order
we believed terminal or unknown) and the order must go through reconciliation
before anything else touches it.
"""

from __future__ import annotations


class DomainError(Exception):
    """Base for all domain-layer errors."""


class IllegalTransition(DomainError):
    """The requested transition is not in the allowed map — a programming error."""


class RequiresReconciliation(DomainError):
    """Legal-looking evidence arrived in a state where it may not be applied
    directly (e.g. a fill for a terminal or UNKNOWN order). The order must be
    moved to RECONCILING and resolved against the broker first."""


class OverfillViolation(DomainError):
    """A fill would take cumulative filled quantity beyond the order quantity.
    This must never be absorbed silently: it means either a broker fault or a
    local accounting fault, and both demand a halt-and-reconcile."""
