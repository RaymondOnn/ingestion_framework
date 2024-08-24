# Mapping common errors to exit codes:
# 0. Success
# 1. Catch All for Errors
# 2. Invalid Source
# 3. Invalid Destination
# 4. Invalid Pipeline
# 5. Invalid Credentials
# 6. Invalid Configuration
# 7. Invalid Run Configuration
# 8. Invalid Load Configuration
# 9. Invalid Extract Configuration
# 10. Invalid Schema
# 11. Invalid Resource
# 12. Invalid Source Configuration
# 13. Invalid Destination Configuration
# 14. Invalid Normalize Configuration
# 15. Invalid Loader Configuration
# 16. Invalid Extract Info
# 17. Invalid Load Info
# 18. Invalid Normalize Info
# 19. Invalid Step Info
# 20. Invalid Pipeline Trace
# 21. Invalid Pipeline Step Trace
import traceback
from typing import Optional, Type

from src.pipeline.log import logger


def handle_exception(error: Exception, message: str, quiet: bool = False):
    logger.trace(traceback.format_exc().strip().splitlines()[-1])
    logger.error(message)
    if not quiet:
        raise error


class InvalidSourceError(Exception):
    """Custom exception class for invalid data source errors."""

    # Constants
    _level = "critical"  # Error level
    _exit_code = 2  # Exit code for the error

    def __init__(self, message="Invalid data source error occurred."):
        super().__init__(message)


class InvalidConfigError(Exception):
    """Custom exception class for invalid config errors."""
    def __init__(self, message) -> None:
        super().__init__()
        self.message = message

class InvalidConfig(Exception):
    def __init__(
        self, message: str, exceptions: list[InvalidConfigError] = []
    ) -> None:
        """
        Initializes the InvalidConfigError exception.

        Args:
            message (str): The error message.
            exceptions (Optional[List[Exception]]): A list of exceptions
                that caused this error. Defaults to None.
        """

        # Call the parent class (Exception) constructor
        super().__init__()

        # Set the error message
        self.message = message

        # Set the exceptions that caused this error
        self.exceptions = exceptions
