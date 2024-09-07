import re
from datetime import datetime
from pathlib import Path

from src.actions.errors import ActionError
from src.pipeline.workdir import Outputs
from src.utils.log import logger

# import parse


def save_to_file(name, key, output_dir, outputs, data, **kwargs):
    filepath = str(Path(output_dir) / outputs[key])
    with open(filepath, "w") as f:
        f.write(str(data))
    Outputs.log(task=name, key=key, value=filepath)


def read_from_file(inputs, **kwargs) -> str:
    name = kwargs["depends_on"]
    key = inputs[name]
    filepath = str(Outputs.get(task=name, key=key))
    with open(filepath) as f:
        data = f.read()
    return data
