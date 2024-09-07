import shutil
from pathlib import Path

from loguru import logger

from src.actions.base import ActionFactory
from src.actions.utils.fs import get_files
from src.decorators import log


@ActionFactory.register("files.import")
@log
def get_file_from_folder(source, pattern, work_dir, **kwargs) -> None:
    files = get_files(source.base_path, pattern)

    counter = 0
    for file in files:
        counter += 1
        shutil.copy(file, work_dir)

    logger.success(
        f"Imported {counter} files into {Path(work_dir).relative_to(Path.cwd())}"
    )
