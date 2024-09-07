from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Any
from typing import Callable
from typing import List


class ClientError(Exception):
    """Base exception class for clients."""


class Client(ABC):
    """Base class for clients."""

    def __init__(self, **kwargs: Any) -> None:
        """Constructor."""
        self.is_source = True

    def __repr__(self) -> str:
        """Return a string representation of the client."""
        return (
            f"{self.__class__.__name__}("
            + ", ".join(
                f"{key}={value}"
                for key, value in vars(self).items()
                if not key.startswith("_")
            )
            + ")"
        )


class Database(Client):
    """Base class for database clients."""

    def __init__(self) -> None:
        """Constructor."""
        super().__init__()
        self.connection = self._get_connection()

    @abstractmethod
    def _get_connection(self) -> Any:
        """Get a connection to the database."""
        ...

    def test_connection(self) -> bool:
        """Test if the connection to the database is working."""
        return bool(self.query("SELECT 1"))

    @abstractmethod
    def query(self, query: str, ddl: bool = False) -> Any:
        """Execute a query on the database."""
        ...

    @abstractmethod
    def create_table(
        self,
        table: str,
        schema: List[dict[str, Any]],
        sort_key: str | None = None,
        is_stg: bool = False,
    ) -> None:
        """Create a table in the database."""
        ...

    @abstractmethod
    def copy_from_file(self, table: str, schema: list, file: Path) -> None:
        """Copy data from a file into a table."""
        ...

    @abstractmethod
    def load_to_prod(
        self,
        source_table: str,
        target_table: str,
        schema: list[dict[str, Any]],
        partition_column: str,
        partition_value: str,
    ) -> None:
        """Load data into production table."""
        ...

    @staticmethod
    def _get_column_info_from_schema(
        schema: List[dict[str, Any]],
        func: Callable,
        sort_key: str | None = None,
    ) -> List[str]:
        """Get column information from a schema."""
        if sort_key:
            schema = sorted(schema, key=lambda x: x[sort_key])
        return list(map(func, schema))

    def _get_load_mode(self, target_table):
        load_mode = "snapshot"
        if self.query(f"SELECT 1 FROM {target_table} LIMIT 1"):
            load_mode = "incremental"

        return load_mode

    @abstractmethod
    def get_row_count(self, table: str, filter_cond: str) -> int:
        """Get the number of rows in a table."""
        ...

    @abstractmethod
    def commit(self) -> None:
        """Commit changes to the database."""
        ...

    @abstractmethod
    def rollback(self) -> None:
        """Roll back changes to the database."""
        ...
