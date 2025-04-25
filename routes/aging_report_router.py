from typing import Annotated

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, UploadFile
from fastapi.params import Depends

from containers import RootContainer
from services.aging_report_service import AgingReportService

aging_reports_router = APIRouter(
    prefix="/aging-reports",
    tags=["aging-reports"],
)


@aging_reports_router.post("/process-file")
@inject
async def process_file(
        file: UploadFile,
        service: AgingReportService = Depends(Provide[RootContainer.aging_report_service])
):
    """
    Process an uploaded binary file.

    Args:
        file: The binary file to process
        service: Aging report service with injected logger

    Returns:
        JSONResponse: A response indicating the result of processing

    Raises:
        HTTPException: Not implemented exception
    """
    return await service.process_uploaded_file(file)
