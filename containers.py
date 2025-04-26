import logging

from dependency_injector import containers, providers
from services.aging_report_service import AgingReportService
from services.inventory_service import InventoryService
from services.multi_logging import LoggingService, LogConfig


class RootContainer(containers.DeclarativeContainer):
    """Root container for application dependencies."""

    wiring_config = containers.WiringConfiguration(packages=["routes"])

    # Services
    aging_report_service = providers.Singleton(
        AgingReportService
    )

    inventory_service = providers.Singleton(
        InventoryService
    )
