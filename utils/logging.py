# utils/logging.py
import logging
import sys
from typing import Dict, Any, Optional
from functools import lru_cache
from pydantic import BaseModel

class LogConfig(BaseModel):
    """Configuration for logging"""
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    level: int = logging.INFO
    log_file: Optional[str] = None
    log_to_console: bool = True

@lru_cache()
def get_logger(name: str, config: Optional[LogConfig] = None) -> logging.Logger:
    """
    Get a logger with the specified configuration.
    
    Args:
        name: Name of the logger
        config: Configuration for the logger (optional)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    if config is None:
        config = LogConfig()
        
    logger = logging.getLogger(name)
    logger.setLevel(config.level)
    
    # Clear existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()
    
    formatter = logging.Formatter(config.format)
    
    # File handler
    if config.log_file:
        file_handler = logging.FileHandler(config.log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Console handler
    if config.log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger