import logging
from typing import Any
from typing import Callable
from typing import Type

from src.contexts.client import ClientContext
from src.contexts.client import InvalidConfigError


class ClientNotFound(Exception):
    pass


class InvalidSourceError(Exception):
    pass


class ClientBase:
    def __init__(self, **kwargs):
        """Constructor."""
        self.is_source = True
        pass

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            + ", ".join(
                f"{k}={v}"
                for k, v in vars(self).items()
                if not k.startswith("_")  # noqa
            )
            + ")"
        )


class ClientFactory(ClientContext):
    """
    Factory class that creates the clients based on the configuration.
    """

    registry: dict[str, Type[ClientBase]] = {}
    """ Internal registry for available executors """

    def __init__(self, context: dict[str, str], task_name: str) -> None:
        """
        Initialize the factory with the context and task name.

        :param context: The context containing the configuration.
        :param task_name: The name of the task.
        """
        super().__init__()
        self.partition_value = context.get("partition_value")

        task_config = context.get(task_name)
        self.source_name: str = task_config.get(
            "from", ""
        )  # need to verify yaml configuration
        self.sink_name: str = task_config.get("to", "")

    @classmethod
    def register(cls, name: str) -> Callable:
        """Class method to register Executor class to the internal registry.
        Args:
            name (str): The name of the executor.
        Returns:
            The Executor class itself.
        """

        def inner_wrapper(wrapped_class: Type[ClientBase]) -> Type[ClientBase]:
            """
            Registers the client class in the registry under the given name.

            Args:
                client_class (Type[ClientBase]): The class to register.

            Returns:
                Type[ClientBase]: The client class itself.
            """
            if name in cls.registry:
                logging.warning(
                    f"Client '{name}' already exists. Will replace it",
                )

            cls.registry[name] = wrapped_class

            return wrapped_class

        return inner_wrapper

    @classmethod
    def create_client(cls, name: str, **kwargs: Any) -> ClientBase:
        """
        Factory command to create the client.

        This method gets the appropriate Client class from the registry and
        creates an instance of it, while passing in the parameters
        given in ``kwargs``.

        Args:
            name (str): The name of the client to create.
            **kwargs (Any): Keyword arguments to pass to the
                client constructor.

        Returns:
            ClientBase: An instance of the client that is created
        """
        if name not in cls.registry:
            raise ClientNotFound(
                f"Client '{name}' does not exist in the registry",
            )
            # return None

        exec_class = cls.registry[name]
        client = exec_class(**kwargs)
        return client

    # def _get_client(self, client_type: str) -> Type[ClientBase]:
    #     """
    #     Get the client class based on the client type.

    #     :param client_type: The type of the client.
    #     :return: The client class if found, otherwise None.
    #     """
    #     return self._client_types.get(client_type, None)

    def get_config(self, client_name: str) -> dict[str, Any]:
        if client_name not in self.configs:
            raise InvalidConfigError(
                f"Client config '{client_name}' does not exist in the registry"
            )

        config = self.configs.get(client_name)
        config["partition_value"] = self.partition_value
        return config

    def get_source(self) -> ClientBase:
        """
        Get the source client based on the configuration.

        Returns:
            The source client instance.
        """
        config = self.get_config(self.source_name)
        client_type = config.pop("type")
        try:
            client = self.create_client(client_type, **config)
            client.is_source = True
            return client
        except Exception as e:
            raise e

    def get_sink(self) -> ClientBase:
        """
        Get the sink client based on the configuration.

        Returns:
            The sink client.
        """
        config = self.get_config(self.sink_name)
        client_type = config.pop("type")
        client = self.create_client(client_type, **config)
        client.is_source = False
        return client
