import logging
from typing import Any
from typing import Callable
from typing import Protocol

from src.exceptions import InvalidSourceError


class ErrorHandler(Protocol):
    """Protocol for ErrorHandler"""

    def __call__(
        self,
        error: Exception,
        context: dict[str, Any],
        next_step: Callable[[Any], None],
    ) -> None:
        """Handle error and continue execution"""
        ...


class SimpleErrorHandler(ErrorHandler):
    """Simple ErrorHandler that raises error"""

    def __call__(
        self,
        error: Exception,
        context: dict[str, Any],
        next_step: Callable[[Any], None],
    ) -> None:
        """Raise error"""
        print("error_handler", error.args, error.__context__, error.__cause__)
        raise error


class ContinueUnlessCritical(ErrorHandler):
    """Pause execution if error is critical"""

    def __call__(
        self,
        error: Exception,
        context: dict[str, Any],
        next_step: Callable[[Any], None],
    ) -> None:

        if isinstance(error, InvalidSourceError):
            logging.critical(msg=f"InvalidSourceError: {error}")
            raise error

        else:
            logging.warn(msg=f"ContinueOnError: {error}")
            next_step(context)
