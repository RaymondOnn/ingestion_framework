import random
import sys
from pprint import pprint
from typing import Any

from src.clients.base import ClientFactory
from src.decorators import log_execution
from src.decorators import retry
from src.decorators import timer
from src.pipeline.pipeline import Pipeline
from src.pipeline.pipeline import PipelineStep
from src.pipeline.pipeline import TNextStep


class StepOne(PipelineStep):
    task_name = "transport"

    @retry(max_tries=5, delay_seconds=1)
    @timer
    @log_execution
    def __call__(self, context: dict[str, Any], next_step: TNextStep) -> None:
        # print("Step One")
        # pprint(context[self.task_name])

        if random.random() < 0.90:
            raise Exception("Something went wrong")

        source, sink = self.get_clients(context)
        # print(source, '\n', sink)

        next_step(context)

    def get_clients(self, context: dict):
        c = ClientFactory(context, self.task_name)
        return c.get_source(), c.get_sink()


@timer
@log_execution
def StepTwo(context: dict[str, Any], next_step: TNextStep) -> None:
    def get_clients(context: dict, task_name: str):
        c = ClientFactory(context, task_name)
        return c.get_source(), c.get_sink()

    TASK_NAME = "process"
    print("Step Two")
    pprint(context[TASK_NAME])

    source, sink = get_clients(context, TASK_NAME)
    # print(source, '\n', sink)

    next_step(context)


def main():
    Pipeline(StepOne(), StepTwo).run("2023-04-31")


if __name__ == "__main__":
    sys.exit(main())
