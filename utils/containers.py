from dependency_injector import containers, providers
import logging
from utils.logging import get_logger, LogConfig

class LoggingContainer(containers.DeclarativeContainer):
    """Container for logging dependencies."""
    
    config = providers.Factory(
        LogConfig,
        format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
        level=logging.INFO,
        log_file="logs/aging_report.log",
        log_to_console=True
    )
    
    aging_report_logger = providers.Factory(
        get_logger,
        name="aging_report",
        config=config
    )
