from datetime import datetime
import time

import pytz

from src.clients.base import ClientFactory
from src.actions.base import ActionFactory


def sleep(secs):
    print(f"Sleeping {secs} secs")
    time.sleep(secs)

def get_clients(**kwargs):
    c = ClientFactory(**kwargs)
    return c.get_source(), c.get_sink()

# @ActionFactory.register("action_one")
# class ActionOne:
#     def __call__(self, **kwargs) -> None:

#         print("action one: Starting")
#         print("kwargs:", kwargs)
#         time.sleep(2)
#         print("action one: Ending")


    # def _execute_step(self, source, target) -> None:
    #     print("action one: Starting")
    #     print("kwargs:", kwargs)
    #     # print("source:", source)
    #     # print("target:", target)
    #     time.sleep(2)
    #     print("action one: Ending")

@ActionFactory.register("action_one")
def ActionTwo(**kwargs) -> None:
    name = kwargs.get("name", None)
    print(name, f"Starting at {datetime.now(tz=pytz.utc)}")
    print("kwargs:", kwargs)
    sleep(2)
    print("action one: Ending")

#     next_step(context)