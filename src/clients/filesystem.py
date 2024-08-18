from pathlib import Path

from src.clients.base import Client
from src.clients.base import ClientFactory
from src.clients.base import SourceClientError

# import watchdog

# .set_active_path(partition_value):
# exists() if source, mkdir() if target


@ClientFactory.register("directory")
class Directory(Client):
    """
    A client for interacting with a directory in the file system.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initializes a new Directory object.

        Args:
            directory_path (str): The path to the directory.
            partition_value (str): The partition value for the directory.
        """

        self.base_path = Path(kwargs.get("directory_path", None))

    def set_active_path(self, partition_value):
        self.active_path = self.base_path / "partition_value"
        self.partition_value = "partition_value"
        if not self.active_path.exists():
            if self.is_source:
                raise SourceClientError(
                    f"Directory '{self.active_path}' does not exist."
                )
            else:
                print(f"Directory '{self.active_path}' not found. Creating...")
                self.active_path.mkdir(parents=True)

    def get_path(self) -> Path:
        """
        Returns the path to the active directory.

        Returns:
            Path: The path to the active directory.
        """
        return self.active_path

    def count(self, file_extension: str) -> int:
        """
        Returns the number of files with the specified file extension
        in the active directory.

        Args:
            file_extension (str): The file extension to count.

        Returns:
            int: The number of files with the specified file extension.
        """
        return len(self.list_files(file_extension))

    def list_files(self, file_extension: str) -> list[Path]:
        """
        Returns a list of file paths with the specified file extension
        in the active directory.

        Args:
            file_extension (str): The file extension to search for.

        Returns:
            list[Path]: A list of file paths with the specified file extension.
        """
        return list(self.active_path.glob(f"*.{file_extension}"))


@ClientFactory.register("snowflake_landing_zone")
class SnowflakeLandingZone(Directory):
    """
    A client for interacting with a Snowflake landing directory
    in the file system.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initializes a new SnowflakeLanding object.

        Args:
            directory_path (str): The path to the directory.
            partition_value (str): The partition value for the directory.
        """
        super().__init__(
            directory_path=kwargs.get("directory_path"),
            partition_value=kwargs.get("partition_value"),
        )
        self.active_path = (
            self.base_path / f"{kwargs.get('partition_value')}-snowflake"
        )  # noqa
