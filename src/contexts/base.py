from pathlib import Path
from loguru import logger
import yaml

from src.contexts.errors import InvalidConfigError


def load_yaml(file_path: str) -> dict:
    logger.info(f"Loading config file: {file_path}")
    try:
        with open(file_path) as f:
            return yaml.safe_load(f)  # json.load(f)
    except yaml.YAMLError as err:
        raise InvalidConfigError(f"Error loading config file: {err}")
    except FileNotFoundError as err:
        raise InvalidConfigError(f"Config file not found: {err}")
    
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