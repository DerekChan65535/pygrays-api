from dependency_injector import containers, providers
from services.aging_report import AgingReportService
from utils.containers import LoggingContainer

class ServicesContainer(containers.DeclarativeContainer):
    """Container for all services."""
    
    # Dependencies from other containers
    logging = providers.DependenciesContainer()
    
    # Service providers
    aging_report_service = providers.Factory(
        AgingReportService,
        logger=logging.aging_report_logger
    )

class AppContainer(containers.DeclarativeContainer):
    """Application container."""
    
    # Include the logging container
    logging = providers.Container(LoggingContainer)
    
    # Services container with injected logging dependencies
    services = providers.Container(
        ServicesContainer,
        logging=logging
    )
