"""Logging configuration for ETL."""
from loguru import logger

logger.add(
    'etl.log', format='{time} {level} {message}', level='INFO', rotation='1 MB', compression='zip',
)
