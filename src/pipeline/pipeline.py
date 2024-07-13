from pathlib import Path
from typing import Any
from typing import Callable
from typing import Optional
from typing import Protocol
from typing import runtime_checkable

from src.contexts.pipeline import ExecutionContext
from src.contexts.pipeline import JobReport
from src.contexts.pipeline import PipelineContext
from src.pipeline.errors import ErrorHandler
from src.pipeline.errors import SimpleErrorHandler

TNextStep = Callable[[Any], None]


class PipelineError(Exception):
    """PipelineError to handle errors in pipeline"""

    pass


@runtime_checkable
class PipelineStep(Protocol):
    """Protocol for PipelineStep"""

    def __call__(self, context: dict[str, Any], next_step: TNextStep) -> None:
        """
        Executes the pipeline step.

        Args:
            context (dict[str, Any]): The context object containing the
                information needed for the step.
            next_step (Callable[[Any], None]): The function to call the next
                step in the pipeline.

        Returns:
            None: This function does not return anything.
        """
        ...


class PipelineCursor(ExecutionContext):
    """PipelineCursor to execute steps in pipeline"""

    def __init__(
        self,
        steps: list[PipelineStep],
        error_handler: ErrorHandler,
    ) -> None:
        """
        Initialize PipelineCursor.

        Args:
            steps (list[PipelineStep]): List of steps to execute.
            error_handler (ErrorHandler): ErrorHandler to handle errors.
        """
        super().__init__()
        self.queue: list[PipelineStep] = steps
        self.error_handler: ErrorHandler = error_handler

    def __call__(self, context: dict) -> None:
        """
        Execute next step in pipeline.

        Args:
            context (Context): Context object.
        """
        if not self.queue:
            return

        # If outputs are not in context, create them
        if "outputs" not in list(context.keys()):
            context = self.__make_step_context(context)

        current_task = self.queue[0]
        next_step = PipelineCursor(self.queue[1:], self.error_handler)

        try:
            current_task(context, next_step)
        except Exception as error:
            self.error_handler(error, context, next_step)

    def __make_step_context(self, context: dict) -> dict:
        """
        Create step context.

        Args:
            context (dict): Context object.

        Returns:
            dict: Updated context object.
        """
        context["outputs"] = {}
        for task_name in self.config.keys():
            context[task_name] = {}
            context[task_name] = self.config.get(task_name)
        return context


# def _default_error_handler(
# error: Exception,
# context: Context,
# next_step: Callable[[Any], None]
# ) -> None:
#     """
#     Default error handler that raises error.

#     Args:
#         error (Exception): Error to raise.
#         context (Context): Context object.
#         next_step (Callable[[Any], None]): Callable to call next step
#           in pipeline.
#     """
#     raise error


class Pipeline(PipelineContext):
    """Pipeline to execute steps in pipeline"""

    def __init__(self, *steps: PipelineStep) -> None:
        """
        Initialize Pipeline.

        Args:
            *steps (PipelineStep): Steps to execute.
        """
        super().__init__()
        self.queue: list[PipelineStep] = [step for step in steps]

    def append(self, step: PipelineStep) -> None:
        """
        Append step to pipeline.

        Args:
            step (PipelineStep): Step to append.
        """
        self.queue.append(step)

    def run(
        self,
        partition_value: str,
        error_handler: Optional[ErrorHandler] = None,
    ) -> None:
        """
        Execute steps in pipeline.

        Args:
            partition_value (str): Partition value.
            error_handler (Optional[ErrorHandler]): ErrorHandler to handle
                errors (default: SimpleErrorHandler).
        """
        job_report = self._start_job_report()
        # self.current_work_dir = self.work_dir / "partition_value"
        # self._init_work_dir(self.current_work_dir)
        self.declare(partition_value, job_report)

        # Execute steps here
        execute = PipelineCursor(
            self.queue,
            error_handler or SimpleErrorHandler(),
        )
        execute(
            {
                "partition_value": partition_value,
                "timestamp_format": self.ts_fmt,
            }
        )

    def __len__(self) -> int:
        """
        Return number of steps in pipeline.

        Returns:
            int: Number of steps.
        """
        return len(self.queue)

    def _init_work_dir(self, work_dir: Path) -> None:
        """
        Initialize work directory.

        Args:
            work_dir (Path): Work directory.
        """
        work_dir.mkdir(parents=True, exist_ok=True)

    def _start_job_report(self) -> JobReport:
        """
        Start job report.

        Returns:
            JobReport: Job report.
        """
        return JobReport()

    def declare(self, partition_value: str, job_report: JobReport) -> None:
        """
        Declare run information.

        Args:
            partition_value (str): Partition value.
            job_report (JobReport): Job report.
        """
        print("*" * 100)
        print("Configs initialized, starting ingestion")
        print(
            f"Ingestion timestamp: \
            {job_report.start_ts.strftime(self.ts_fmt)}"
        )
        print("Partition Value: ", partition_value)
        # print(f"Log Path: {self.work_dir / self.log_file_name}")
        print("*" * 100)

    def _report_pipeline_run(self, job_report: JobReport) -> None:
        """
        Report pipeline job results

        Args:
            job_report (JobReport): Job report.
        """
        job_report.log_end_time()

        duration_secs = int(
            (job_report.end_ts - job_report.start_ts).total_seconds(),
        )
        hours, remainder = divmod(duration_secs, 3600)
        minutes, seconds = divmod(remainder, 60)
        job_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        print("*" * 100)
        print("Pipeline run completed")
        print(f"Start time: {job_report.start_ts.strftime(self.ts_fmt)}")
        print(f"End time: {job_report.end_ts.strftime(self.ts_fmt)}")
        print(f"Duration: {job_duration}")
        print(f"Exit code: {job_report.exit_code}")
        print(f"Log Path: {self.work_dir / self.log_file_name}")
        print("*" * 100)
