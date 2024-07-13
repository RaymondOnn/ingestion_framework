from src.contexts.base import load_config


class InvalidConfigError(Exception):
    pass


class ClientContext:
    """
    Context class that holds the configuration for the clients.
    """

    def __init__(self):
        """
        Initialize the context with the configuration from the config file.
        """
        self.configs = load_config()["locations"]
