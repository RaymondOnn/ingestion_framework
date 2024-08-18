from collections import defaultdict
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from datetime import timezone
from typing import NoReturn
from typing import Optional
from typing import Self

from src.actions.base import ActionFactory
from src.contexts.pipeline import AppContext
from src.contexts.pipeline import ExecutionContext
from src.contexts.pipeline import JobReport
from src.contexts.pipeline import Node
from src.pipeline.errors import ErrorHandler
from src.pipeline.errors import SimpleErrorHandler
from src.pipeline.log import JobLogHandler
from src.pipeline.log import logger
from src.pipeline.workdir import WorkingDirectory


class PipelineError(Exception):
    """PipelineError to handle errors in pipeline"""

    pass


class PipelineCursor(ExecutionContext):
    def __init__(
        self,
        dag_graph: dict,
        task_dependencies: dict,
        error_handler,
        log_handler: JobLogHandler,
        working_directory: WorkingDirectory,
    ) -> None:
        super().__init__()
        self.graph = dag_graph
        self.deps = task_dependencies
        self.error_handler = error_handler
        self.log_handler = log_handler
        self.workdir = working_directory

    def get_node(self, step_name: str):
        context = self.steps.get(step_name)
        params = context.pop("params", {})
        context.update({"name": step_name})
        return Node(name=step_name, context=context, params=params)

    def _validate_node(self, step_name: str):
        """Validate source and target before proceeding"""
        raise NotImplementedError

    # TODO: Add step for checking source and destination before
    # TODO: Check for starting conditions before starting
    # proceeding i.e connection, empty source
    def execute(
        self,
        executor: "ThreadPoolExecutor",
        step_name: str,
        partition_value: str,
    ):
        node = self.get_node(step_name)
        action_name = node.context.get("uses", None)

        if action_name:
            action = ActionFactory.setup_action(action_name)
            params = node.params
            params.update(
                {
                    "partition_value": partition_value,
                    "name": step_name,
                    "work_dir": str(self.workdir.object),
                    "output_dir": str(self.workdir.output_dir),
                }
            )
            info = node.context | params

            # create new record in log table
            self.log_handler.create(step_name, params)
            self.log_handler.start()
            try:
                future = executor.submit(action, **info)
                self.log_handler.success()
                return future
            except Exception as error:
                # FIXME: failed() only equipped to handle strings and
                # not Exceptions
                self.log_handler.failed(error)
                self.error_handler(error, params)

    def _process_result(self, result):
        """
        Provides an opportunity to maintain information outside
        the execution of the pipeline

        """
        pass

    def __call__(self, partition_value: str) -> list[int]:
        """
        Performs a topological sort of the graph using Khan's algorithm.
        This function uses parallelization to speed up the execution time by
        executing independent nodes concurrently.

        Returns:
        --------
        List[int]:
            A list of nodes in topological order.
        """
        deps = self.deps.copy()
        q = []

        with ThreadPoolExecutor() as executor:

            # Add all nodes with in-degree 0 to the queue
            for step_name in self.graph:
                if deps[step_name] == 0:

                    # Add a tuple with node and its execution to the queue
                    q.append(
                        (
                            step_name,
                            self.execute(
                                executor,
                                step_name,
                                partition_value,
                            ),
                        )
                    )

            # Initialize an empty list to hold the sorted nodes
            result = []

            # Keep sorting until the queue is empty
            while q:
                for step_name, execution in q:

                    # If the execution is not done, continue the loop
                    if not execution.done():
                        continue

                    # Remove the executed node from the queue and
                    # add it to the result
                    self._process_result(execution.result())
                    q.remove((step_name, execution))
                    result.append(step_name)

                    # Decrement the in-degree of all adjacent nodes
                    for neighbor in self.graph[step_name]:
                        deps[neighbor] -= 1

                        # Add the neighbor to the queue if its in-degree is 0
                        if deps[neighbor] == 0:
                            q.append(
                                (
                                    neighbor,
                                    self.execute(
                                        executor,
                                        neighbor,
                                        partition_value,
                                    ),
                                )
                            )
        return result


class Pipeline(AppContext):
    """Pipeline to execute steps in pipeline"""

    def __init__(self) -> None:
        """
        Initializes a new empty graph.
        """
        super().__init__()
        self.graph: dict = defaultdict(list)
        self.deps: dict = defaultdict(int)

    # TODO: Would this work if I want to specify the yaml config to use?
    def generate_dag(self) -> Self:
        logger.info("Generating DAG...")

        for name, context in self.execution_context.steps.items():
            self.add(name, context.get("depends_on", None))
        logger.success("Generated DAG successfully")
        return self

    def add(self, step: str, depends_on: Optional[str] = None) -> bool:
        """
        Adds a directed edge from node u to node v.

        Parameters:
        -----------
        u: int
            The starting node of the edge.
        v: int
            The ending node of the edge.

        Returns:
        --------
        bool
            True if the edge is added successfully,
            False if the edge would create a cycle.
        """
        _ = self.graph[step]
        if depends_on is None:
            return True

        if isinstance(depends_on, list):
            for dep in depends_on:
                self._register(step, dep)
        else:
            self._register(step, depends_on)

        return True

    def _register(
        self,
        step: str,
        depends_on: Optional[str] = None,
    ) -> bool | NoReturn:
        # Edge already exists or creates a cycle
        if depends_on == step:
            logger.warning(
                f"Edge already exists between {depends_on} and {step}"
            )  # noqa

        if step in self.graph[depends_on]:
            raise PipelineError(
                f"Illegal cycle detected between {depends_on} and {step}"
            )

        # Temporarily add the edge to detect cycles
        self.graph[depends_on].append(step)
        cycle_exists = self.detect_cycle()
        if cycle_exists:
            # If a cycle is created, remove the edge and return False
            self.graph[depends_on].remove(step)
            error_msg = (
                f"Illegal cycle detected between {depends_on} and {step}"  # noqa
            )
            raise PipelineError(error_msg)

        # If no cycle is created, add the edge and update in-degree
        self.graph[depends_on].append(step)
        self.deps[step] += 1

        return True

    def detect_cycle(self) -> bool:
        """
        Detects cycles in the graph using a depth-first search algorithm.

        Returns:
        --------
        bool
            True if a cycle exists, False otherwise.
        """
        visited = set()

        def dfs(node, stack=None):
            stack = set() if stack is None else stack

            visited.add(node)
            stack.add(node)

            for neighbor in self.graph[node]:
                if neighbor not in visited:
                    if dfs(neighbor, stack):
                        return True
                elif neighbor in stack:
                    return True

            stack.remove(node)
            return False

        for node in list(self.graph):
            if node not in visited:
                if dfs(node):
                    return True

        return False

    def run(
        self,
        partition_value: str,
        error_handler: Optional[ErrorHandler] = None,
        log_handler: Optional[JobLogHandler] = None,
        working_directory: Optional[WorkingDirectory] = None,
    ) -> None:
        self.declare(partition_value, self._start_job_report())
        self.setup()

        execute = PipelineCursor(
            self.graph,
            self.deps,
            error_handler or SimpleErrorHandler(),
            log_handler or self.log_handler,
            working_directory or self.workdir,
        )
        execute(partition_value)

        self.teardown()

    def topological_sort(self) -> list[int]:
        """
        Performs a topological sort of the graph using Khan's algorithm.

        Returns:
        --------
        List[int]:
            A list of nodes in topological order.
        """
        result = []
        q: deque = deque()

        # Add all nodes with in-degree 0 to the queue
        for node in self.graph.keys():
            if self.deps[node] == 0:
                q.append(node)

        while q:
            # Remove a node from the queue and add it to the result
            node = q.popleft()
            result.append(node)

            # Decrement the in-degree of all adjacent nodes
            for neighbor in self.graph[node]:
                self.deps[neighbor] -= 1

                # Add the neighbor to the queue if its in-degree is 0
                if self.deps[neighbor] == 0:
                    q.append(neighbor)

        # Check if there was a cycle in the graph
        if len(result) != len(self.graph):
            raise ValueError("Graph contains a cycle")

        return result

    def __len__(self) -> int:
        """
        Return number of steps in pipeline.

        Returns:
            int: Number of steps.
        """
        return len(self.graph.keys())

    def _start_job_report(self) -> JobReport:
        """
        Start job report.

        Returns:
            JobReport: Job report.
        """
        return JobReport()

    def declare(self, partition_value: str, job_report: JobReport) -> None:
        """
        Declare pipeline configuration

        Args:
            partition_value (str): Partition value.
            job_report (JobReport): Job report.
        """
        print("*" * 100)
        logger.info("Configs initialized, Starting ingestion")
        logger.info(f"Job Name: {self.pipeline_context.job_name}")
        logger.info(
            f"Ingestion timestamp: {job_report.start_ts.strftime(self.pipeline_context.ts_fmt)}"  # noqa
        )  # noqa
        logger.info(f"Partition Value: {partition_value}")
        # logger.info(f"Log Path: {self.work_dir / self.log_file_name}")

    def setup(self) -> None:
        # Start logger for job logs
        self.log_handler = JobLogHandler("current_execution")
        logger.success("Log table connected")

        # Create a temporary working directory
        dir_name = (
            self.pipeline_context.job_name
            + "_"
            + datetime.now(timezone.utc).astimezone().strftime("%Y%m%d%H%M%S")
        )
        self.workdir = WorkingDirectory.create(directory_name=dir_name)
        print("*" * 100)

    def conclude(self):
        logger.info("*" * 100)
        logger.info("*" * 100)

    def teardown(self) -> None:
        # Remove the temporary working directory
        self.workdir.remove()

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
