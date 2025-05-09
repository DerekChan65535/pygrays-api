from typing import Annotated

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, UploadFile, File
from fastapi.params import Depends
import fastapi.responses
from starlette.responses import HTMLResponse, JSONResponse

from containers import RootContainer
from services.aging_report_service import AgingReportService

aging_reports_router = APIRouter(
    prefix="/aging-reports",
    tags=["aging-reports"],
)

import logging
from typing import List
from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse
import io

from models.response_base import ResponseBase
from models.file_model import FileModel
from services.aging_report_service import AgingReportService
from services.multi_logging import LoggingService

# Initialize router
router = APIRouter(prefix="/aging-report", tags=["Aging Report"])

# Initialize logger
logger = LoggingService().get_logger(__name__)

# Initialize service
aging_report_service = AgingReportService()


def get_aging_report_service():
    """Dependency to get the aging report service instance"""
    return aging_report_service


@router.post("/process")
async def process_aging_report(
    mapping_file: UploadFile = File(...),
    data_files: List[UploadFile] = File(...),
    service: AgingReportService = Depends(get_aging_report_service)
):
    """
    Process aging report files and return combined report
    
    Args:
        mapping_file: CSV file containing mapping tables
        data_files: List of CSV files containing daily sales data with state info in filenames
        service: AgingReportService instance
        
    Returns:
        Processed Excel file as a downloadable response or error details
    """
    try:
        logger.info(f"Processing aging report with {len(data_files)} data files")
        
        # Convert UploadFile objects to FileModel instances
        mapping_file_model = FileModel(
            name=mapping_file.filename,
            content=await mapping_file.read()
        )
        
        data_file_models = []
        for file in data_files:
            data_file_models.append(
                FileModel(
                    name=file.filename,
                    content=await file.read()
                )
            )
        
        # Process the files
        response: ResponseBase = await service.process_uploaded_file(
            mapping_file=mapping_file_model,
            data_files=data_file_models
        )
        
        # If processing failed, return the error response
        if not response.is_success:
            logger.error(f"Failed to process aging report: {response.errors}")
            return {
                "is_success": False,
                "errors": response.errors,
                "data": None
            }
        
        # If successful, return the file as a downloadable response
        result_file: FileModel = response.data
        
        return StreamingResponse(
            io.BytesIO(result_file.content),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={result_file.name}"
            }
        )
        
    except Exception as e:
        logger.error(f"Error in process_aging_report endpoint: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while processing the aging report: {str(e)}"
        )
@aging_reports_router.post("/process-file")
@inject
async def process_file(
        state: str,
        mapping_file: Annotated[UploadFile, File(description="CSV mapping file")],
        data_file: Annotated[UploadFile, File(description="CSV data file")],
        service: AgingReportService = Depends(Provide[RootContainer.aging_report_service])
):
    mapping_file_model = FileModel(mapping_file.filename, await mapping_file.read())
    data_file_model = FileModel(data_file.filename, await data_file.read())
    
    response = await service.process_uploaded_file(state, mapping_file_model, data_file_model)
    
    if response.is_success:
        return fastapi.responses.Response(
            content=response.data.content,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={response.data.name}"
            }
        )
    else:
        return JSONResponse(
            status_code=400,
            content=response.to_dict()
        )


@aging_reports_router.post("/files/")
async def create_files(files: Annotated[list[bytes], File()]):
    return {"file_sizes": [len(file) for file in files]}


@aging_reports_router.post("/uploadfiles/")
async def create_upload_files(
        files: Annotated[list[UploadFile], File(description="Multiple files as UploadFile")],
):
    if not files:
        return {"error": "No files uploaded"}
    
    first_file = files[0]
    file_content = await first_file.read()
    
    return fastapi.responses.Response(
        content=file_content,
        media_type=first_file.content_type,
        headers={
            "Content-Disposition": f"attachment; filename={first_file.filename}"
        }
    )


@aging_reports_router.get("/")
async def content_html():
    content = """
        <body>
        <form action="/aging-reports/files/" enctype="multipart/form-data" method="post">
        <input name="files" type="file" multiple>
        <input type="submit">
        </form>
        <form action="/aging-reports/uploadfiles/" enctype="multipart/form-data" method="post">
        <input name="files" type="file" multiple>
        <input type="submit">
        </form>
        <form action="/aging-reports/process-file" enctype="multipart/form-data" method="post">
        <h3>Process Sales Aged Balance Report</h3>
        <div>
            <label for="state">State: </label>
            <input name="state" type="text" required>
        </div>
        <div>
            <label for="mapping_file">Mapping File (CSV): </label>
            <input name="mapping_file" type="file" required>
        </div>
        <div>
            <label for="data_file">Data File (CSV): </label>
            <input name="data_file" type="file" required>
        </div>
        <input type="submit" value="Process">
        </form>
        </body>
    """
    return HTMLResponse(content=content)
