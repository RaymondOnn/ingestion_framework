import socket
from pathlib import Path
from typing import Optional

import yaml


def get_environment() -> str:
    """
    Retrieves the environment based on the hostname.

    This function uses the `socket` module to get the hostname and
        extracts the last character.
    It then maps the last character to a corresponding environment string.
    If the last character is not recognized, an `EnvironmentError` is raised
        with the unknown hostname.

    Returns:
        str: The environment string corresponding to the hostname.

    Raises:
        EnvironmentError: If the hostname does not end with 'p', 'c', 'u',
            or 'd'.

    """
    # hostname = socket.gethostname().lower().split(".")[0]
    # environment_mapping = {
    #     "p": "prd",
    #     "c": "ppr",
    #     "u": "uat",
    #     "d": "dev",
    # }
    # environment = environment_mapping.get(hostname[-1], None)
    # if environment is None:
    #     raise OSError(
    #         f"Unknown environment: {hostname}. \
    #         Host name must end with 'p/c/u/d'"
    #     )
    # return environment
    return "test"


DEFAULT_CONFIG_FILE_PATH = Path("src/config") / f"{get_environment()}.yaml"


def load_config(file_path: Optional[Path] = None) -> dict:
    """Loads yaml configuration file.

    Args:
        file_path (Optional[pathlib.Path], optional): Path to the config file
            to load. Defaults to None.

    Returns:
        dict: The loaded yaml configuration.
    """
    if file_path is None:
        file_path = DEFAULT_CONFIG_FILE_PATH
    try:
        with open(file_path) as f:
            return yaml.safe_load(f)

    except yaml.YAMLError as err:
        raise Exception(f"Error loading config file: {err}")

    except FileNotFoundError as err:
        raise Exception(f"Config file not found: {err}")
