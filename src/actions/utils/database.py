from pathlib import Path
from typing import Any, Callable, NoReturn

import polars as pl
from loguru import logger

from src.actions.utils.fs import create_temp_dir, move_files
from src.clients.base import Database


def get_row_count(file) -> int:
    lazy_df = pl.scan_csv(file, has_header=True)
    return lazy_df.select(pl.len()).collect().item()


def get_columns(schema: list[dict[str, Any]], func: Callable) -> list[str]:
    schema = sorted(schema, key=lambda x: x["index"])
    return list(map(func, schema))


def run_load_step(
    working_dir,
    file_format,
    target_db: Database,
    target_table,
    schema,
    partition_column,
    partition_value,
) -> bool | NoReturn:
    logger.info(
        f">> Executing load step for {partition_column}={partition_value} <<"
    )
    rows = []  # for storing row count of each file
    STG_TABLE = f"STG_{target_table}"
    loaded = False

    try:
        # move files that match criteria to temporary directory
        tmpdir = create_temp_dir(working_dir)
        move_files(working_dir, tmpdir.name, file_format, partition_value)

        # copy files to staging table
        files = sorted(list(Path(tmpdir.name).iterdir()))
        logger.debug(
            f"{len(files)} file objects found in source directory: {[file.name for file in files]}"
        )
        for file in files:
            if file.stat().st_size:
                target_db.copy_from_file(
                    table=target_table, schema=schema, file=file
                )
                loaded = True
                logger.info(
                    f"Loaded file {file.name} into table STG_{target_table.upper()}"
                )
                rows.append(get_row_count(file.absolute()))
            else:
                logger.warning(f"Skipped empty file {file.name}")

        if not loaded: 
            return False

        # check row counts
        expected = sum(rows)
        # FIXME: table name issue
        actual = target_db.get_row_count(
            STG_TABLE,
            f"DATE_TRUNC('day', TO_TIMESTAMP(invoice_date, 'YYYY-MM-DD HH24:MI:SS')) = '{partition_value}'",
        )

        if actual == expected:
            # if passed, promote staging table to production table
            logger.success(
                f"Rows inserted in Temp Table {STG_TABLE} [{actual}] == Expected rows [{expected}]"
            )
            target_db.load_to_prod(
                STG_TABLE,
                target_table,
                schema,
                partition_column,
                partition_value,
            )
            target_db.commit()
            return True
        else:
            logger.error(
                f"Rows inserted in Temp Table {STG_TABLE} [{actual}] <> Expected rows [{expected}]"
            )
            logger.error(
                f"Loading into table {target_table} failed for {partition_column}={partition_value}"
            )
            target_db.rollback()
            return False

    except Exception as e:
        print("Error")
        logger.error(
            f"Loading into table {target_table} failed for {partition_column}={partition_value}"
        )
        logger.error("Rolling back changes...")
        target_db.rollback()
        # move files back to working directory
        move_files(tmpdir.name, working_dir, file_format, partition_value)
        tmpdir.cleanup()  # delete temporary directory
        # return False
        logger.exception(e)  # print error message
