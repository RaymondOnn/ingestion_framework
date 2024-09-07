class InvalidConfigError(Exception):
    """Custom exception class for invalid config errors."""

    def __init__(self, message) -> None:
        super().__init__()
        self.message = message


class InvalidConfig(Exception):
    def __init__(self, message: str, exceptions: list[InvalidConfigError] = []) -> None:
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