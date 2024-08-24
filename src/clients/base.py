
from pprint import pprint
from typing import Any
from typing import Callable
from typing import NoReturn
from typing import Type

from src.contexts.pipeline import ClientContext
from src.exceptions import InvalidConfigError
from src.pipeline.log import logger


class ClientNotFound(Exception):
    pass


class SourceClientError(Exception):
    pass


class Client:
    def __init__(self, **kwargs):
        """Constructor."""
        self.is_source = True

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

    def __str__(self) -> str:
        return self.__repr__()

class ClientFactory:
    """
    Factory class that creates the clients based on the configuration.
    """

    registry: dict[str, Type[Client]] = {}
    """ Internal registry for available executors """

    def __init__(
        self,  
        client_ctx: ClientContext,
        partition_value: str, 
        **kwargs: Any
    ) -> None:
        """
        Initialize the factory with the context and task name.

        :param context: The context containing the configuration.
        :param task_name: The name of the task.
        """
        self.client_ctx = client_ctx
        self.partition_value = partition_value

    @classmethod
    def register(cls, name: str) -> Callable:
        """Class method to register Executor class to the internal registry.
        Args:
            name (str): The name of the executor.
        Returns:
            The Executor class itself.
        """

        def inner_wrapper(wrapped_class: Type[Client]) -> Type[Client]:
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
    def create_client(cls, type: str, **kwargs: Any) -> Client | NoReturn:
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
        if type not in cls.registry:
            raise ClientNotFound(
                f"Client '{type}' does not exist in the registry",
            )

        exec_class = cls.registry[type]
        client = exec_class(**kwargs)
        return client

    # def _get_client(self, client_type: str) -> Type[ClientBase]:
    #     """
    #     Get the client class based on the client type.

    #     :param client_type: The type of the client.
    #     :return: The client class if found, otherwise None.
    #     """
    #     return self._client_types.get(client_type, None)

    @staticmethod
    def get_client_config(
        client_ctx: ClientContext,  name: str, partition_value: str
    ) -> dict[str, Any] | NoReturn:
        
        client_dict = client_ctx.clients
        if name not in client_dict:
            msg = f"Client config '{name}' does not exist in the registry"
            logger.error(msg)
            raise InvalidConfigError(msg)
            
        config = client_dict.get(name, {})
        config.update({"partition_value": partition_value})
        return config
        

    def get_source(self, name: str) -> Client | NoReturn:
        """
        Get the source client based on the configuration.

        Returns:
            The source client instance.
        """
        try:
            config = self.get_client_config(
                self.client_ctx, name, self.partition_value,
            )
            client_type = config.pop("type")
            client = self.create_client(client_type, **config)
            client.is_source = True  #?: is it necessary?
            return client
        except Exception as e:
            raise e

    def get_target(self, name) -> Client:
        """
        Get the target client based on the configuration.

        Returns:
            The target client.
        """
        try:
            config = self.get_client_config(
                self.client_ctx, name, self.partition_value,
            )
            client_type = config.pop("type")
            client = self.create_client(client_type, **config)
            client.is_source = False
            return client
        except Exception as e:
            raise e
    
    def __repr__(self):
        return f"{self.__class__.__name__}(partition_value={self.partition_value})"
