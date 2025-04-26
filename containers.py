import logging

from dependency_injector import containers, providers
from services.aging_report_service import AgingReportService
from services.inventory_service import InventoryService
from services.multi_logging import LoggingService, LogConfig


class LoggingContainer(containers.DeclarativeContainer):
    """Container for logging-related dependencies."""

    config = providers.Factory(
        LogConfig,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        log_file=None,
        log_to_console=True
    )

    service = providers.Singleton(
        LoggingService,
        default_config=config
    )

    logger = providers.Factory(
        service.provided.get_logger,
        name="app"
    )


class RootContainer(containers.DeclarativeContainer):
    """Root container for application dependencies."""

    # Configure logging
    logging = providers.Container(LoggingContainer)

    # Services
    aging_report_service = providers.Singleton(
        AgingReportService,
        logger=logging.logger
    )

    inventory_service = providers.Singleton(
        InventoryService,
        logger=logging.logger
    )
