from enum import Enum
import duckdb
from dataclasses import dataclass

@dataclass
class DuckdbConfig:
    db_file: str
    
class DuckdbClient:
    def __init__(self, config: DuckdbConfig) -> None:
        self.conn = duckdb.connect(config.db_file)

    def __enter__(self):
        return self
    
    def __exit__(self):
        self.conn.close()
    

class Database(Enum):
    duckdb = DuckdbClient(
        DuckdbConfig(db_file="memory.duckdb")
    )
    snowflake = ""
    