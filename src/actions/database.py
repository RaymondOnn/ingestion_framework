
from pprint import pprint
from typing import Any
from loguru import logger
from datetime import datetime


from src.actions.base import ActionFactory
from src.actions.utils.database import run_load_step
from src.actions.utils.fs import extract_rundates_from_files

from src.decorators import log

@ActionFactory.register("database.copy")
@log
def load_to_database(
    target,
    target_table: str,
    work_dir: str, 
    partition_column: str,
    partition_value: str,  # none: load all dates / date: load specific date / list: load specific dates / range: load specific dates
    filename_date_pattern, # how to extract run_date from filename
    file_format: str,
    schema: list[dict[str, Any]],
    **kwargs,
) -> dict[str, Any]:
    # pprint(kwargs)
    completed_dates = set()
    failed_dates = set()
    
    # get dates
    logger.info(f"Partition_value: {"None" if not partition_value else partition_value}")
    if partition_value:
        run_dates = [ \
            datetime.strptime(partition_value, "%Y-%m-%d") \
                .strftime("%Y-%m-%d")
        ]
        logger.info(f"Setting Run Dates as: {run_dates}")
    else:
        run_dates = extract_rundates_from_files(work_dir, filename_date_pattern)
    
    
    for run_date in run_dates:
        result = run_load_step(
            working_dir=work_dir, 
            file_format=file_format, 
            partition_column=partition_column,
            partition_value=run_date, 
            target_db=target,
            target_table=target_table,
            schema=schema,
        )
        
        if result:
            completed_dates.add(run_date)
        else:
            failed_dates.add(run_date)
            logger.warning(f"No loading done for run_date={run_date}")
        
    metadata = {
        "completed": completed_dates,
        "failed": failed_dates
    }
    
    return metadata