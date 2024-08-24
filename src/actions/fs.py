
import shutil
from typing import Any

from src.actions.base import ActionFactory
from src.actions.blocks import get_files
from src.pipeline.log import log





@ActionFactory.register("files.import")
@log
def get_file_from_folder(source, work_dir, **kwargs) -> dict[str, Any]:
    files = get_files(source.base_path)
    for file in files:
        shutil.copy(file, work_dir)
    
    return {"status": "success"}