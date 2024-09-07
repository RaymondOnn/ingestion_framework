from dataclasses import dataclass


import duckdb


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