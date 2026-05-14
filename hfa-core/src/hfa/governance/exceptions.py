"""
hfa-core/src/hfa/governance/exceptions.py
IRONCLAD — BudgetGuard exceptions
"""


class BudgetGuardError(Exception):
    """Base exception for BudgetGuard errors."""

    pass


class BudgetExhaustedError(BudgetGuardError):
    """Raised when budget is insufficient for operation."""

    pass


class BudgetLockedError(BudgetGuardError):
    """Raised when budget is locked by another operation."""

    pass


class BudgetNotFoundError(BudgetGuardError):
    """Raised when tenant budget does not exist."""

    pass


class BudgetInvalidAmountError(BudgetGuardError):
    """Raised when amount is invalid (negative, zero, or non-integer)."""

    pass
