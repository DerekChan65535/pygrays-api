from typing import Callable
from fastapi import Depends

from services.aging_report_service import AgingReportService

# Initialize service instances
aging_report_service = AgingReportService()

# Define dependency providers
def get_aging_report_service() -> AgingReportService:
    """
    Dependency provider for the AgingReportService.
    
    Returns:
        An instance of the AgingReportService
    """
    return aging_report_service

