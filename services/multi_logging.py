import sys
import logging
from dataclasses import dataclass
from typing import Optional


@dataclass
class LogConfig:
    """Configuration for logging."""
    level: int = logging.INFO
    fmt: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_file: Optional[str] = None
    log_to_console: bool = True


class LoggingService:
    """Service for configuring and providing loggers."""

    def __init__(self, default_config: Optional[LogConfig] = None):
        """Initialize the logging service with default configuration."""
        self.default_config = default_config or LogConfig()

    def get_logger(self, name: str, config: Optional[LogConfig] = None) -> logging.Logger:
        """
        Retrieve a configured logger instance.
        Args:
            name: The name of the logger.
            config: Optional custom configuration (defaults to default_config).
        Returns:
            A configured logger instance.
        """
        effective_config = config or self.default_config
        logger = logging.getLogger(name)
        logger.setLevel(effective_config.level)
        if logger.hasHandlers():
            logger.handlers.clear()
        formatter = logging.Formatter(effective_config.fmt)
        if effective_config.log_file:
            logger.addHandler(self._create_file_handler(effective_config.log_file, formatter))
        if effective_config.log_to_console:
            logger.addHandler(self._create_console_handler(formatter))
        return logger

    def _create_file_handler(self, log_file: str, formatter: logging.Formatter) -> logging.Handler:
        """Create a file handler with the provided formatter."""
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        return file_handler

    def _create_console_handler(self, formatter: logging.Formatter) -> logging.Handler:
        """Create a console handler with the provided formatter."""
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        return console_handler
