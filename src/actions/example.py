# Specification for actions
# - Must be a function
# - Must return dict
# - If there are outputs, they must be returned in a dict 'artifacts
import time
from pathlib import Path
from typing import Any

from src.actions.base import ActionFactory
from src.actions.blocks import get_files
from src.actions.blocks import read_from_file
from src.actions.blocks import save_to_file
from src.pipeline.log import log


def sleep(secs):
    print(f"Sleeping {secs} secs")
    time.sleep(secs)


# @ActionFactory.register("unarchiver")
@log
def unpack(directory_path, work_dir, **kwargs):
    from zipfile import ZipFile
    from tarfile import tarfile

    def extract_files(path, to_directory):
        if path.endswith(".zip"):
            file = ZipFile(path, "r")
        else:
            if path.endswith(".tar.gz") or path.endswith(".tgz"):
                opener, mode = tarfile.open, "r:gz"
            elif path.endswith(".tar.bz2") or path.endswith(".tbz"):
                opener, mode = tarfile.open, "r:bz2"
            else:
                raise ValueError(
                    f"Could not extract `{path}` as no appropriate extractor is found"  # noqa
                )

            file = opener(path, mode)

        # loading the temp.zip and creating a zip object
        try:
            # Extracting all the members of the zip
            # into a specific location.
            file.extractall(to_directory)
        except Exception as e:
            raise e
        finally:
            file.close()

    files = get_files(directory_path, **kwargs)
    for file in files:
        filename = Path(file).name
        extract_files(file, Path(work_dir).joinpath(filename))


@ActionFactory.register("action_one")
@log
def ActionOne(secs: int, **kwargs) -> None:
    # pprint(kwargs)
    save_to_file(key="test1", data=secs, **kwargs)


@ActionFactory.register("action_two")
@log
def ActionTwo(**kwargs) -> dict[str, Any]:
    # pprint(kwargs)
    data = int(read_from_file(**kwargs))
    sleep(data)
    return {"status": "success"}


@ActionFactory.register("action_three")
@log
def ActionThree(secs: int, **kwargs) -> dict[str, Any]:
    sleep(secs)
    return {"status": "success"}


#     next_step(context)
