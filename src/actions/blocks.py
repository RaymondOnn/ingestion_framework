from pathlib import Path

from src.clients.base import ClientFactory
from src.pipeline.workdir import Outputs

# import parse


def get_clients(kwargs):
    c = ClientFactory(**kwargs)
    return c.get_source(), c.get_sink()


def get_files(
    directory_path: str, file_name: str = "", pattern: str = "*"
) -> list[str]:
    return [
        str(p) for p in Path(directory_path).joinpath(file_name).glob(pattern)
    ]  # noqa


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
