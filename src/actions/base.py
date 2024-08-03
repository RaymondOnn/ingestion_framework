import logging
from typing import Any, Callable, NoReturn, Type, Protocol


class ActionNotFound(Exception):
    pass

class Action(Protocol):
    
    def __call__(self):
        pass



class ActionFactory:
    """
    Factory class that creates the clients based on the configuration.
    """

    registry: dict[str, Callable[[Any], Any]] = {}
    """ Internal registry for available executors """

    @classmethod
    def register(cls, name: str) -> Callable:
        """Class method to register Executor class to the internal registry.
        Args:
            name (str): The name of the executor.
        Returns:
            The Executor class itself.
        """

        def inner_wrapper(wrapped_class: Callable[[Any], Any]) -> Callable[[Any], Any]:
            """
            Registers the client class in the registry under the given name.

            Args:
                client_class (Type[Action]): The class to register.

            Returns:
                Type[Action]: The client class itself.
            """
            if name in cls.registry:
                logging.warning(
                    f"Action '{name}' already exists. Will replace it",
                )

            cls.registry[name] = wrapped_class

            return wrapped_class

        return inner_wrapper

    @classmethod
    def setup_action(cls, name: str) -> Callable[[Any], Any] | NoReturn:
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
            Action: An instance of the client that is created
        """
        if name not in cls.registry:
            raise ActionNotFound(
                f"Action '{name}' does not exist in the registry",
            )
            # return None

        # action = cls.registry[name]
        # return action
        return cls.registry[name]