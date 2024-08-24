from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


from src.exceptions import InvalidConfig, InvalidConfigError
from src.pipeline.log import logger


DEFAULT_THREAD_COUNT = 10
DEFAULT_PROCESS_COUNT = -1
DEFAULT_TS_FMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_DATE_FMT = "%Y-%m-%d"
DEFAULT_LOG_FILE_NAME = "logs/pipeline.log"
DEFAULT_CODE_DIR = Path.cwd() / "src"
DEFAULT_WORK_DIR = Path.cwd()


# TODO: Add support for inputs and outputs to faciliate communication between nodes  # noqa
@dataclass
class Node:
    name: str
    context: dict[str, Any]
    params: dict[str, Any]

@dataclass
class JobContext:
    name: str
    retries: int = field(default=3)
    retry_delay_secs: int = field(default=5)
    timeout_secs: int = field(default=300)
    # self.work_dir = Path(cfg.get("working_directory", DEFAULT_WORK_DIR))
    # self.code_dir = Path(
    # cfg.get("code_directory"), DEFAULT_CODE_DIR
    # )
    
    max_thread_count: int = field(default=DEFAULT_THREAD_COUNT)
    max_process_count: int = field(default=DEFAULT_PROCESS_COUNT)
    ts_fmt: str = field(default=DEFAULT_TS_FMT)
    date_fmt: str = field(default=DEFAULT_DATE_FMT)
    extras_cfg: dict = field(default_factory=dict)
    log_file_name: str = field(default=DEFAULT_LOG_FILE_NAME)
    def post_init(self, timestamp_format: str):
        try:
            self.ts_fmt = timestamp_format
            self._validate()
        except InvalidConfig as err:
            for exception in err.exceptions:
                logger.error(str(exception.message))

            logger.info("Invalid config. Unable to continue. Exiting...")
            raise SystemExit(1)


    def _validate(self):
        errors = []

        if self.name is None:
            errors.append(
                InvalidConfigError(message="Job name is required"),
            )

        if errors:
            raise InvalidConfig(message="Invalid config", exceptions=errors)


@dataclass(frozen=True)
class WorkflowContext:
    steps: dict[str, Any]


@dataclass(frozen=True)
class ClientContext:
    clients: dict[str, Any] = field(default_factory=dict)
    
    def post_init(self):
        try:
            self._validate()
        except InvalidConfig as err:
            for exception in err.exceptions:
                logger.error(str(exception.message))

            logger.info("Invalid config. Unable to continue. Exiting...")
            raise SystemExit(1)


    def _validate(self):
        if self.clients is None:
            raise InvalidConfigError(message="ClientContext is required: {self.clients}")


# class AppContext:
#     execution_context: ExecutionContext = ExecutionContext()
#     pipeline_context: PipelineContext = PipelineContext()


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
