from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NoReturn, Optional, Self

import yaml

from src.actions.base import ActionFactory
from src.clients.base import ClientFactory
from src.contexts.pipeline import (
    ClientContext,
    JobContext,
    JobReport,
    Node,
    WorkflowContext,
)
from src.contexts.settings import DEFAULT_CONFIG_FILE_PATH
from src.exceptions import InvalidConfigError
from src.pipeline.errors import ErrorHandler, SimpleErrorHandler
from src.pipeline.log import JobLogHandler, logger
from src.pipeline.workdir import WorkingDirectory


def is_yaml_file(file_path: str) -> bool:
    result = False
    try:
        # Check that specified config file exists
        assert Path(file_path).exists()
        assert Path(file_path).is_file()

        # Check that specified config file is a yaml file
        assert Path(file_path).suffix == ".yaml"
        result = True
    except AssertionError:
        msg = f"Config file is not a yaml file: {file_path}"
        logger.error(msg)
        raise InvalidConfigError(msg)
    return result


def load_yaml(file_path: str) -> dict:
    logger.info(f"Loading config file: {file_path}")
    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f)  # json.load(f)
    except yaml.YAMLError as err:
        raise InvalidConfigError(f"Error loading config file: {err}")
    except FileNotFoundError as err:
        raise InvalidConfigError(f"Config file not found: {err}")


class PipelineError(Exception):
    """PipelineError to handle errors in pipeline"""

    pass


class PipelineCursor:
    def __init__(
        self,
        dag_graph: dict,
        task_dependencies: dict,
        workflow_ctx: WorkflowContext,
        client_ctx: ClientContext,
        error_handler: ErrorHandler,
        log_handler: JobLogHandler,
        working_directory: WorkingDirectory,
    ) -> None:
        self.graph = dag_graph
        self.deps = task_dependencies
        self.clients = client_ctx
        self.steps = workflow_ctx.steps
        self.error_handler = error_handler
        self.log_handler = log_handler
        self.workdir = working_directory

    def get_node(self, step_name: str):
        context = self.steps.get(step_name, {})
        params = context.pop("params", {})
        context.update({"name": step_name})
        return Node(name=step_name, context=context, params=params)

    def _validate_node(self, step_name: str):
        """Validate source and target before proceeding"""
        raise NotImplementedError

    def _get_source_client(self, source_name: str, partition_value: str):
        factory = ClientFactory(self.clients, partition_value)
        return factory.get_source(source_name)

    def _get_target_client(self, target_name: str, partition_value: str):
        factory = ClientFactory(self.clients, partition_value)
        return factory.get_target(target_name)

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
        if action_name is None:
            msg = f"Unable to get value from key 'uses' for step '{step_name}':{action_name}"
            logger.error(msg)
            raise PipelineError(msg)

        if action_name:
            action = ActionFactory.setup_action(action_name)
            params = node.params

            # add additional info to params
            params.update(
                {
                    "partition_value": partition_value,
                    "name": step_name,
                    "work_dir": str(self.workdir.object),
                    "output_dir": str(self.workdir.output_dir),
                }
            )

            # swap source and target name to actual clients if any
            if "source" in params:
                params["source"] = self._get_source_client(
                    params["source"], partition_value
                )
            if "target" in params:
                params["target"] = self._get_target_client(
                    params["target"], partition_value
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


class Pipeline:
    """Pipeline to execute steps in pipeline"""

    def __init__(self, config_file: str | None = None) -> None:
        """
        Initializes a new empty graph.
        """
        self.graph: dict = defaultdict(list)
        self.deps: dict = defaultdict(int)

        if config_file is None:
            config_file = DEFAULT_CONFIG_FILE_PATH

        if is_yaml_file(config_file):
            cfg = load_yaml(config_file)
            self.config_file = config_file

        self.workflow_ctx = self._get_workflow_ctx(cfg)
        self.client_ctx = self._get_client_ctx(cfg)
        self.job_ctx = self._get_job_ctx(cfg)

    # TODO: Would this work if I want to specify the yaml config to use?
    @classmethod
    def from_yaml(cls, config_file: str | None = None) -> Self:
        return cls(config_file)

    def generate_dag(self, steps: WorkflowContext) -> Self:
        logger.info("Generating DAG from config file")
        for name, context in steps.steps.items():
            self.add(name, context.get("depends_on", None))
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
            logger.warning(f"Edge already exists between {depends_on} and {step}")  # noqa

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
            error_msg = f"Illegal cycle detected between {depends_on} and {step}"  # noqa
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
        print("*" * 100)
        self.declare(partition_value, self._start_job_report())
        self.setup()
        print("*" * 100)
        execute = PipelineCursor(
            dag_graph=self.graph,
            task_dependencies=self.deps,
            workflow_ctx=self.workflow_ctx,
            client_ctx=self.client_ctx,
            error_handler=error_handler or SimpleErrorHandler(),
            log_handler=log_handler or self.log_handler,
            working_directory=working_directory or self.workdir,
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

    def _get_workflow_ctx(self, config_dict: dict) -> WorkflowContext | NoReturn:
        try:
            steps = config_dict.get("steps")
            assert steps
            return WorkflowContext(steps=steps)
        except (KeyError, AssertionError):
            msg = f"Key 'steps' not found in config: {self.config_file}"
            logger.error(msg)
            raise InvalidConfigError(msg)

    def _get_job_ctx(self, config_dict: dict) -> JobContext:
        labels = ["steps", "locations"]
        for label in labels:
            if label in config_dict:
                config_dict.pop(label)

        config_dict["ts_fmt"] = config_dict.pop("timestamp_format")
        return JobContext(**config_dict)

    def _get_client_ctx(self, config_dict: dict) -> ClientContext:
        try:
            clients = config_dict.get("locations")
            assert clients
            return ClientContext(clients=clients)
        except (KeyError, AssertionError):
            msg = "Client config not found"
            logger.error(msg)
            raise InvalidConfigError(msg)

    def declare(self, partition_value: str, job_report: JobReport) -> None:
        """
        Declare pipeline configuration

        Args:
            partition_value (str): Partition value.
            job_report (JobReport): Job report.
        """
        logger.info("Configs initialized, Starting ingestion")
        logger.info(f"Job Name: {self.job_ctx.name}")
        logger.info(
            f"Ingestion timestamp: {job_report.start_ts.strftime(self.job_ctx.ts_fmt)}"  # noqa
        )  # noqa
        logger.info(f"Partition Value: {partition_value}")

    def setup(self) -> None:
        self.generate_dag(steps=self.workflow_ctx)

        # Start logger for job logs
        self.log_handler = JobLogHandler("current_execution")
        logger.success("Log table connected")

        # Create a temporary working directory
        dir_name = (
            self.job_ctx.name
            + "_"
            + datetime.now(timezone.utc).astimezone().strftime("%Y%m%d%H%M%S")
        )
        self.workdir = WorkingDirectory.create(directory_name=dir_name)
        logger.info(f"Log Path: {self.workdir.object /  self.job_ctx.log_file_name}")

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
        print(f"Start time: {job_report.start_ts.strftime(self.job_ctx.ts_fmt)}")
        print(f"End time: {job_report.end_ts.strftime(self.job_ctx.ts_fmt)}")
        print(f"Duration: {job_duration}")
        print(f"Exit code: {job_report.exit_code}")
        print(f"Log Path: {self.workdir + self.job_ctx.log_file_name}")
        print("*" * 100)

    @property
    def clients(self) -> dict[str, Any]:
        return self._clients
