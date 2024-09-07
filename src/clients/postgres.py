from pathlib import Path
from typing import Any, NoReturn

import psycopg
from loguru import logger
from psycopg import conninfo, sql

from src.clients.base import ClientError, Database
from src.clients.factory import ClientFactory

ALLOWED_FORMATS = ["csv", "json"]


@ClientFactory.register("postgres")
class Postgres(Database):
    def __init__(
        self,
        user: str,
        password: str,
        database: str = "postgres",
        host: str = "localhost",
        port: str = "5432",
        **kwargs,
    ) -> None:
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = self._get_connection()

    def _get_connection(self) -> psycopg.Connection | NoReturn:
        try:
            config = {
                "dbname": self.database,
                "user": self.user,
                "password": self.password,
                "host": self.host,
                "port": self.port,
            }
            conn_str = conninfo.make_conninfo(**config)
            logger.info(
                f"Connecting to postgresql with connection string: {conn_str}"
            )
            conn = psycopg.connect(
                conninfo=conn_str,
            )
            if self.test_connection(conn):
                return conn
            else:
                raise ClientError("Unable to connect to Postgres")
        except Exception as e:
            logger.error(e)
            raise ClientError(e)

    def __enter__(self):
        return self

    def __exit__(self):
        if self.conn:
            self.conn.close()

    def test_connection(self, conn):
        result = self.query(conn, "SELECT version()")
        if not result:
            return False
        logger.info(f"Postgres version: {result[0][0]}")
        return bool(result)

    def query(self, conn, query, quiet: bool = False) -> Any | None:
        with conn.cursor() as cursor:
            try:
                logger.debug(f"Executed query: {query}")
                cursor.execute(query)
                
                if not quiet:
                    logger.info(
                        f"{cursor.rowcount} rows returned"
                        if cursor.rowcount > 0
                        else "No rows returned"
                    )
                results = cursor.fetchall()
                return results  
            except Exception as e:
                logger.error(e)
                cursor.execute("ROLLBACK;")
                
                

    def execute(self, conn, query, auto_commit: bool = True):
        with conn.cursor() as cursor:
            # data_ops = ['DELETE', 'UPDATE', 'INSERT']
            # is_data_ops = any(x in query for x in data_ops)
            # if is_data_ops and auto_commit:
            #     cursor.execute("START TRANSACTION;")
            try:
                logger.info(f"Executed query: {query}")
                ret = cursor.execute(query)
            except Exception as e:
                logger.error(e)
                # logger.error("Rolling back changes...")
                cursor.execute("ROLLBACK;")
            
            cursor.execute("COMMIT;")
            return ret

    def create_table(
        self,
        table: str,
        schema: list,
        sort_key: str | None = None,
        is_stg: bool = False,
        auto_commit: bool = True,
    ):
        query = self._generate_create_table_query(
            table=table, schema=schema, is_stg=is_stg, sort_key=sort_key
        )
        self.execute(self.conn, query)
        logger.info(f"Created table {table}")

    def _generate_create_table_query(
        self,
        table: str,
        schema: list,
        sort_key: str | None = None,
        is_stg: bool = False,
    ) -> str:
        table_name = "STG_" + table if is_stg else table
        drop_qry = f"DROP TABLE IF EXISTS {table_name};"
        if is_stg:
            # import data as varchar first
            cols = self._get_column_info_from_schema(
                schema=schema,
                func=lambda x: x["target_name"] + " " + "VARCHAR",
                sort_key=sort_key,
            )
        else:
            cols = self._get_column_info_from_schema(
                schema=schema,
                func=lambda x: x["target_name"] + " " + x["data_type"].upper(),
                sort_key=sort_key,
            )
        create_qry = f"""
            CREATE TABLE {table_name} (
                {"\n\t\t, ".join(cols)}
            );        
        """

        query = "\n".join([drop_qry, create_qry])
        return query

    def copy_from_file(
        self, table: str, schema: list, file: Path, auto_commit: bool = True
    ) -> None:
        # TODO: Add support for partitioned tables
        ext = file.suffix[1:] if file.suffix[0] == "." else file.suffix
        if ext in ALLOWED_FORMATS:
            self.create_table(table, schema, is_stg=True)
            query = self._generate_copy_query(table, schema, file)
            try:
                with open(str(file)) as f:
                    with self.conn.cursor().copy(sql.SQL(query)) as copy:
                        copy.write(f.read())
                if auto_commit:
                    self.commit()
                logger.info(f"Executed query: {query}, file: {file.name}")
            except Exception as e:
                raise e
        else:
            raise ClientError(f"File format not supported: {file.suffix}")

    def _generate_copy_query(
        self, table: str, schema: list, file: Path
    ) -> str:
        ext = file.suffix[1:] if file.suffix[0] == "." else file.suffix
        col_str = "\n\t\t, ".join(
            self._get_column_info_from_schema(
                schema=schema,
                func=lambda x: x["target_name"],
                sort_key="source_index",
            )
        )
        copy_qry = f"""
            COPY STG_{table}(
                {col_str}
            ) 
            FROM STDIN 
            WITH(FORMAT {ext}, HEADER TRUE);
        """.upper()
        return copy_qry

    def load_to_prod(
        self,
        source_table: str,
        target_table: str,
        schema: list[dict[str, Any]],
        partition_column: str,
        partition_value: str,
    ) -> None:
        # Probe to check if table exists. Returns None if table does not exist
        query = f"SELECT * FROM {target_table} LIMIT 1"
        ret = self.query(self.conn, query, quiet=True)
        
        if not ret:
            logger.info(f"Table {target_table} does not exist, creating it...")
            self.create_table(
                table=target_table,
                schema=schema,
                sort_key="target_index",
            )

        cols_str = "\n\t\t\t, ".join(
            self._get_column_info_from_schema(
                schema=schema,
                func=lambda x: "CAST("
                + x["target_name"]
                + " AS "
                + x["data_type"]
                + ") AS "
                + x["target_name"],
                sort_key="target_index",
            )
        )
        query = f"""
            DELETE FROM {target_table} WHERE {partition_column} = '{partition_value}';
            INSERT INTO {target_table} 
                SELECT 
                    {cols_str} 
                FROM {source_table};        
        """
        self.execute(self.conn, query)

        logger.info(
            f"Loaded partition {partition_column}={partition_value} into {target_table}"
        )

    def get_row_count(
        self, table: str, filter_cond: str = "TRUE"
    ) -> int | NoReturn:
        query = f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE {filter_cond};
        """
        ret = self.query(self.conn, query)
        if ret:
            return int(ret[0][0])
        else:
            raise ClientError(f"Table {table} does not exist: {ret}")

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()


# sql = '''CREATE TABLE DETAILS(employee_id int NOT NULL,\
# employee_name char(20),\
# employee_email varchar(30), employee_salary float);'''


# cursor.execute(sql)


# cursor.execute(sql2)

# sql3 = '''select * from details;'''
# cursor.execute(sql3)
# for i in cursor.fetchall():
#     logger.info(i)

# conn.commit()
# conn.close()
# conn.commit()
# conn.close()
