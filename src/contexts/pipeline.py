from datetime import datetime
from pathlib import Path

from src.contexts.base import load_config


class PipelineContext:
    def __init__(self) -> None:
        cfg = load_config()["workflow"]
        self.config = cfg
        # self.work_dir = Path(cfg.get("working_directory", "."))
        print(Path.cwd() / "src")
        # self.code_dir = Path(cfg.get("code_directory"), Path.cwd() / "src")
        self.max_thread_count = cfg.get("max_thread_count", None)
        self.max_process_count = cfg.get("max_process_count", None)
        self.ts_fmt = cfg.get("timestamp_format", "%Y-%m-%d %H:%M:%S")
        self.date_fmt = cfg.get("date_format", "%Y-%m-%d")
        self.extras_cfg = cfg.get("extras", {})
        self.log_file_name = cfg.get("log_file_name", "logs/pipeline.log")


class ExecutionContext:
    def __init__(self) -> None:
        cfg = load_config()["workflow"]["steps"]
        self.config = cfg


class JobReport:
    def __init__(self) -> None:
        self.start_ts: datetime = datetime.now()
        self.end_ts: datetime = None
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
