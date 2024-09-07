import re
import tempfile
from datetime import datetime
from pathlib import Path

from loguru import logger

from src.actions.errors import ActionError
from src.actions.utils.date import is_date


def get_files(
    directory_path: str,
    pattern: str = "*",
) -> set[str]:
    """
    Get a set of files matching a given pattern from a directory.

    To select a specific file, set pattern = name of the file i.e. test.csv
    To select by extension, set pattern = "*.csv"
    For multiple patterns, use regex i.e. pattern = r"*.csv|*.txt"
    Otherwise, the pattern will match any file and will return all files

    Args:
    - directory_path (str): The path to the directory to search in.
    - pattern (str): The pattern to match files against. Defaults to "*".

    Returns:
    - set[str]: A set of file paths that match the pattern.
    """
    logger.debug(
        f"Calling 'get_files' with directory_path={directory_path}, pattern={pattern}"
    )

    # Check if the directory exists
    path = Path(directory_path)
    if not path.exists():
        msg = f"Path '{path.absolute()}' does not exist"
        logger.error(msg)
        raise ActionError(msg)

    # Check if the directory is a directory
    if not path.is_dir():
        msg = f"Path '{path.absolute()}' is not a directory"
        logger.error(msg)
        raise ActionError(msg)

    logger.info(f"Getting files from '{path.absolute()}'")

    # Get a set of files matching the pattern
    files: set[str] = set()
    for item in path.glob(r"**/*"):
        f_path = str(item)
        match = re.match(pattern=pattern, string=f_path)
        if match:
            # Add the file path to the set
            files.add(f_path)

    logger.debug(f"{len(files)} files found in '{path.absolute()}'")
    return files


def move_files(
    source_dir: str,
    target_dir: str,
    file_extension: str = "",
    partition_value: str | None = None,
) -> None:
    # ^.*?(20240429).*?(\.csv)$  ([\w]{0,}?)$
    logger.debug(f"Calling move_files(source_dir={source_dir}, \
target_dir={target_dir}, file_extension={file_extension}, \
partition_value={partition_value})")

    source_folder = Path(source_dir)
    target_folder = Path(target_dir)
    cwd = Path.cwd()
    partition_value = (
        datetime.strptime(partition_value, "%Y-%m-%d").strftime("%Y%m%d")
        if partition_value is not None
        else partition_value
    )

    # Compile the pattern
    pattern_dict = {
        0: "*",
        1: f"^.*?(\\.{file_extension})$",
        2: f"^.*?({partition_value}).*?(\\.([\\w]{0,}?))$",
        3: f"^.*?({partition_value}).*?(\\.{file_extension})$",
    }
    value = 0
    value += 1 if file_extension != "" else 0
    value += 2 if partition_value is not None else 0
    pattern = re.compile(pattern_dict[value])
    logger.debug(f"Final pattern selected: [ {pattern_dict[value]} ]")

    # Move the files
    moved_files = set()
    source_files = set(source_folder.iterdir())
    logger.debug(f"{len(source_files)} file objects found in source directory")
    logger.debug(
        f"Files found: [\n{(
            ', \n\t\t'.join(sorted([str(Path(file).relative_to(source_folder)) 
            for file in source_files]))
        )} ]"
    )
    for file in source_files:
        if bool(pattern.match(str(file.name))):
            file.rename(target_folder / file.name)
            moved_files.add(file.name)

    logger.info(
        f"Moved {len(moved_files)} files from '{source_folder.relative_to(cwd)}' to '{target_folder.relative_to(cwd)}'"
    )

    # Assert that all files moved' are in target folder
    try:
        # TODO: assert files in target folder
        target_files = {str(file.name) for file in target_folder.iterdir()}
        assert moved_files.issubset(target_files)
        assert moved_files.difference(target_files) == set()
    except AssertionError:
        # FIXME: find files in 'moved' not in 'target'
        missing_files = sorted(moved_files.difference(target_files))
        msg = f"{len(missing_files)} files moved but not found in target directory: {missing_files}"
        missing_files_list = ", \n\t".join(
            [
                str(Path(file).relative_to(target_folder))
                for file in missing_files
            ]
        )
        logger.error(msg)
        logger.error(f"Files found: [{missing_files_list}]")
        raise ActionError(msg)


def create_temp_dir(working_dir) -> tempfile.TemporaryDirectory:
    tempfile.tempdir = working_dir
    tmpdir = tempfile.TemporaryDirectory()
    logger.info(f"Created temporary directory: {tmpdir.name}")
    return tmpdir


def extract_rundates_from_files(
    directory_path, filename_date_pattern
) -> list[str]:
    run_dates = []
    pattern = re.compile(filename_date_pattern)
    for file in Path(directory_path).iterdir():
        match = pattern.match(str(file))
        if match:
            print(
                "date_str: ", filename_date_pattern.format(**match.groupdict())
            )
            if is_date(filename_date_pattern.format(**match.groupdict())):
                run_dates.append(
                    datetime.strptime(file.name, "%Y%m%d").strftime("%Y-%m-%d")
                )
    logger.info("Found files for run_dates: [" + ",".join(run_dates) + "]")
    return run_dates
