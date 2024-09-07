
from pprint import pprint
import yaml
from pathlib import Path
from typing import Any, Optional, Self


from src.utils.log import logger
from src.contexts.errors import InvalidConfigError
from src.utils.classes import Singleton

DEFAULT_CONFIG_FILE_PATH = "src/config/test.yaml"

class JobSettings(Singleton):
    _CONFIG_FILE: Optional[str] = None
    _CONFIG: Optional[dict[str, Any]] = {}
    
    _STEPS: dict[str, Any] = {}
    _CLIENTS: dict[str, Any] = {}
    _GENERAL: dict[str, Any] = {}


    @classmethod
    def from_yaml(cls, yaml_config_file: str | None = None) -> Self:
            if yaml_config_file is None:
                yaml_config_file = DEFAULT_CONFIG_FILE_PATH
                
            try: 
                # Check that specified config file exists
                assert Path(yaml_config_file).exists() 
                assert Path(yaml_config_file).is_file()

                # Check that specified config file is a yaml file
                assert Path(yaml_config_file).suffix == ".yaml"
            except AssertionError:
                msg = f"Config file is not a yaml file: {yaml_config_file}"
                logger.error(msg)
                raise InvalidConfigError(msg)

            logger.info(f"Loading config file: {yaml_config_file}")
            
            cls._CONFIG_FILE = yaml_config_file
            try:
                with open(cls._CONFIG_FILE, 'r') as f:
                    cls._CONFIG = yaml.safe_load(f) # json.load(f)

                
                if cls._CONFIG:    
                    cfg = cls._CONFIG.copy()
                    cls._STEPS = cfg.pop("steps")
                    cls._CLIENTS = cfg.pop("locations")
                    cls._GENERAL = cfg
                return cls()
            except yaml.YAMLError as err:
                raise InvalidConfigError(f"Error loading config file: {err}")
            except FileNotFoundError as err:
                raise InvalidConfigError(f"Config file not found: {err}")
            
        
    @property
    def config_file(self) -> Optional[str]:
        return self._CONFIG_FILE

    # @staticmethod
    # def get_required_config_var(config_var: str) -> str:
    #     assert Settings._CONFIG
    #     if config_var not in Settings._CONFIG:
    #         msg = f"Config variable '{config_var}' not set in config file '{Settings._CONFIG_FILE}'"
    #         raise InvalidConfigError(msg)
    #     return Settings._CONFIG[config_var]
    
    @classmethod
    def get_section(cls, section_name: str) -> dict:
        assert cls._CONFIG
        if getattr(cls, section_name, None) is None:
            pprint(cls._CONFIG)
            if section_name not in cls._CONFIG.keys():
                msg = f"Config section '{section_name}' not set in config file '{cls._CONFIG_FILE}'"
                logger.error(msg)
                raise InvalidConfigError(msg)
            else:
                setattr(cls, section_name, cls._CONFIG[section_name])
        return getattr(cls, section_name)
    
    @classmethod
    def reset(cls) -> None:
        cls._CONFIG_FILE = None
        cls._CONFIG = None
        
    
        
        

# class AppSettings(Settings): 
    # def __init__(self, config_file: str) -> None:
    #     super().__init__(config_file)