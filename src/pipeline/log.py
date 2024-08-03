
# TODO: How to mark failed if dependent task fails
# TODO: Add in more info about the job

import functools
import logging
import time
import uuid
from enum import Enum
from typing import Any, Optional
from datetime import datetime

import duckdb
from dateutil.relativedelta import relativedelta

from src.pipeline.database import Database

logging.basicConfig(level=logging.INFO)

def set_ttl_time(years=1):
    timestamp = datetime.timestamp(datetime.now() + relativedelta(years=years))
    return datetime.fromtimestamp(timestamp)

# TODO: add rows for each step at the start of the job run
class LogTable:
    def __init__(
        self,
        db_conn,
        log_table_name: str
        ) -> None:

        self.changes: dict[str, Any] = {}
        self.table = log_table_name  # "current_execution"
        self.conn = db_conn
        self._init_db()

    def get_conn(self, db_file) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(db_file)

    def _init_db(self) -> bool:
        """Create table if not exists"""
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                run_id VARCHAR 
                , job_name VARCHAR 
                , step_name VARCHAR 
                , status VARCHAR 
                , start_ts TIMESTAMP 
                , end_ts TIMESTAMP 
                , partition_value VARCHAR 
                , params VARCHAR 
                , error_message VARCHAR 
                , log_path VARCHAR 
                , ttl TIMESTAMP
                , created_by VARCHAR DEFAULT 'system' 
                , created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                , last_updated_at TIMESTAMP 
            )
        """
        self.conn.execute(query)
        return True
        
    def get(self, run_id: str) -> tuple[int]:
        query = f"SELECT COUNT(1) FROM {self.table} WHERE run_id = '{run_id}'"
        self.conn.execute(query)
        return self.conn.fetchone()
        
    def save(self, run_id: str, ) -> bool:
        """save to database table"""
        values = list(self.changes.values())
        if self.get(run_id)[0]:
            # update row
            set_str = ", ".join([f"{key} = ?" for key in self.changes.keys()])
            values.extend([run_id])
            query = f"UPDATE {self.table} SET {set_str} WHERE run_id = ?"
        else: 
            
            col_str = ', '.join(self.changes.keys())
            value_str = ','.join(["?"]*len(values))
            query = f"INSERT INTO {self.table}({col_str}) VALUES ({value_str})"
        
        self.conn.execute(query, values)
        self.changes.clear()
        return True

    # def attributes(self):
    #     self.run_id = run_id
    #     self.job_name = job_name
    #     self.log_path = log_path
    #     self.partition_value = partition_value
    #     self.ttl = ttl
    #     self.created_by = created_by
    #     self.created_at = created_at
    #     self.step_name = step_name
    #     self.start_ts = start_ts
    #     self.status = status
    #     self.last_updated_at = last_updated_at
    #     self.params = params
    #     self.end_ts = end_ts
    #     self.error_message = error_message

    # TODO: empty current_execution table into execution_log table
    def clear(self):
        raise NotImplementedError
    
    def set_attr(self, attribute: str, value: Any) -> bool:
        """track new changes to the table"""
        setattr(self, attribute, value)
        self.changes.update({attribute: value})
        return True

class Status(Enum):
    queued = "QUEUED"
    preparing = "PREPARING"
    validating = "VALIDATING"
    running = "RUNNING"
    success = "SUCCESS"
    failed = "FAILED"
    blocked = "BLOCKED"
    skipped = "SKIPPED"
    unknown = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)

class JobLogHandler:
    def __init__(self, log_table_name: str):
        self.table_name = log_table_name
        self.log_table = LogTable(
            db_conn=Database.duckdb.value.conn, 
            log_table_name=log_table_name
        )
        self.run_id = ""

    def create(self, name:str, params: dict, **kwargs):
        # print("inside create task ", name)
        self.run_id = f"{name}#${uuid.uuid4().__str__()}"
        now = str(datetime.now())
        self.log_table.set_attr("run_id", self.run_id)
        self.log_table.set_attr("job_name", name)
        self.log_table.set_attr("step_name", name)
        self.log_table.set_attr("start_ts", now)
        self.log_table.set_attr("ttl", set_ttl_time())
        self.log_table.set_attr("last_updated_at", now)
        self.log_table.set_attr("params", params)
        self.log_table.save(self.run_id)
    
    def failed(self, error_message="Error") -> bool:
        # print("inside failed task ", self.run_id)
        try:
            now = str(datetime.now())
            self.log_table.set_attr("status", str(Status.failed))
            self.log_table.set_attr("error_message", error_message)
            self.log_table.set_attr("end_ts", now)
            self.log_table.set_attr("last_updated_at", now)
            self.log_table.save(self.run_id)
            self.run_id = ""
            return True
        except Exception:
            return False

    def success(self):
        # print("inside success task ", self.run_id)
        try: 
            now = str(datetime.now())
            self.log_table.set_attr("status", str(Status.success))
            self.log_table.set_attr("end_ts", now)
            self.log_table.set_attr("last_updated_at", now)
            self.log_table.save(self.run_id)
            self.run_id = ""
            return True
        except Exception:
            print("failed to close task ")
            return False

    def start(self):
        # print("inside start task ", self.run_id)
        try:
            self.log_table.set_attr("status", str(Status.running))
            self.log_table.save(self.run_id)
            return True
        except Exception:
            return False

    def end(self):
        print("inside end task ", self.run_id)
        try:
            self.log_table.clear()
            return True
        except Exception:
            return False

def log(func):
    """
    Decorator that logs the start and end of a function execution.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: The decorated function.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        """
        Wrapper function that logs the start and end of a function execution.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            The result of the decorated function.
        """
        # Log the start of the function execution
        step = kwargs.get("name", func.__name__) 
        start = time.perf_counter()
        logging.info(f"Executing '{step}' at {datetime.fromtimestamp(start)} with args {args} and kwargs {kwargs}")
        
        try:
            # Call the decorated function
            result = func(*args, **kwargs)

            # Log the end of the function execution
            end = time.perf_counter()
            duration = f"{end - start:.4f}"
            logging.info(f"Finished executing '{step}'. Took {duration} seconds")
            
            return result
        
        except Exception as e:
            response = {"status": -1, "error": {"message": str(e)}}
            raise Exception(response)
        finally:
            # task_instance.end()
            pass

    return wrapper

        