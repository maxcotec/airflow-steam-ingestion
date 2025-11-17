"""
Utility functions for date/time handling and common operations.
"""
import logging
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger(__name__)


def get_run_date_from_logical_date(logical_date: datetime) -> date:
    """
    Extract date from Airflow logical_date.
    
    Args:
        logical_date: Airflow task logical_date (execution_date)
        
    Returns:
        date object representing the run date
    """
    if isinstance(logical_date, datetime):
        return logical_date.date()
    elif isinstance(logical_date, date):
        return logical_date
    else:
        logger.warning(f"Unexpected logical_date type: {type(logical_date)}")
        return datetime.now().date()


def get_run_hour_from_logical_date(logical_date: datetime) -> int:
    """
    Extract hour from Airflow logical_date.
    
    Args:
        logical_date: Airflow task logical_date (execution_date)
        
    Returns:
        Hour as integer (0-23)
    """
    if isinstance(logical_date, datetime):
        return logical_date.hour
    else:
        logger.warning(f"Unexpected logical_date type: {type(logical_date)}")
        return datetime.now().hour


def format_run_date(run_date: date) -> str:
    """
    Format run_date as string in YYYY-MM-DD format.
    
    Args:
        run_date: date object
        
    Returns:
        Formatted date string
    """
    return run_date.strftime("%Y-%m-%d")


def log_dag_run_info(logical_date: datetime, context: dict = None) -> None:
    """
    Log DAG run information from context.
    
    Args:
        logical_date: Airflow task logical_date
        context: Airflow task context
    """
    run_date = get_run_date_from_logical_date(logical_date)
    run_hour = get_run_hour_from_logical_date(logical_date)
    
    logger.info(f"DAG Run - Date: {format_run_date(run_date)}, Hour: {run_hour}")
    
    if context:
        logger.info(f"Task: {context.get('task').task_id}")
        logger.info(f"Execution Date: {context.get('execution_date')}")
        logger.info(f"Try Number: {context.get('task_instance').try_number}")
