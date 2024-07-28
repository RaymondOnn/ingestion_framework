from datetime import datetime
from pathlib import Path
from typing import Any

from src.contexts.base import load_config
from dataclasses import dataclass


DEFAULT_THREAD_COUNT = 10
DEFAULT_PROCESS_COUNT = -1
DEFAULT_TS_FMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_DATE_FMT = "%Y-%m-%d"
DEFAULT_LOG_FILE_NAME = "logs/pipeline.log"
DEFAULT_CODE_DIR = Path.cwd() / "src"
DEFAULT_WORK_DIR = Path.cwd()

@dataclass
class Node: 
    name: str
    context: dict[str, Any]

    
class PipelineContext:
    def __init__(self) -> None:
        cfg = self._get_config()
        # self.work_dir = Path(cfg.get("working_directory", DEFAULT_WORK_DIR))
        # self.code_dir = Path(cfg.get("code_directory"), DEFAULT_CODE_DIR)
        self.max_thread_count = cfg.get("max_thread_count", DEFAULT_THREAD_COUNT)
        self.max_process_count = cfg.get("max_process_count", DEFAULT_PROCESS_COUNT)
        self.ts_fmt = cfg.get("timestamp_format", DEFAULT_TS_FMT)
        self.date_fmt = cfg.get("date_format", DEFAULT_DATE_FMT)
        self.extras_cfg = cfg.get("extras", {})
        self.log_file_name = cfg.get("log_file_name", DEFAULT_LOG_FILE_NAME)
        
    def _get_config(self):
        cfg = load_config()
        cfg.pop("steps")
        cfg.pop("locations")
        return cfg

class ExecutionContext:
    def __init__(self) -> None:
        self.steps = load_config()["steps"]



class AppContext:
    def __init__(self) -> None:
        self.execution_context = ExecutionContext()
        self.pipeline_context = PipelineContext()


class JobReport:
    def __init__(self) -> None:
        self.start_ts: datetime = datetime.now()
        self.end_ts: datetime | None = None
        self.exit_code: int = -1
        self.errors = None  # []

    def __str__(self) -> str:
        return f"Job started at {self.start_ts} and \
            ended at {self.end_ts} with exit code {self.exit_code}"

    def log_end_time(self) -> None:
        self.end_ts = datetime.now()

    def log_exit_code(self, exit_code: int) -> None:
        self.exit_code = exit_code

    def __repr__(self) -> str:
        return str(self)
