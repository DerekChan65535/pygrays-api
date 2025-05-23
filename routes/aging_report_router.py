from fastapi import APIRouter
from typing import List, Optional
from fastapi import UploadFile, File, HTTPException, Depends, Form
from fastapi.responses import StreamingResponse, JSONResponse
import io
from datetime import datetime

from models.response_base import ResponseBase
from models.file_model import FileModel
from services.aging_report_service import AgingReportService
from services.multi_logging import LoggingService

aging_reports_router = APIRouter(
    prefix="/aging-reports",
    tags=["aging-reports"],
)

# Initialize logger
logger = LoggingService().get_logger(__name__)

# Initialize service
aging_report_service = AgingReportService()


def get_aging_report_service():
    """Dependency to get the aging report service instance"""
    return aging_report_service


@aging_reports_router.post("/process")
async def process_aging_report(
    mapping_file: UploadFile = File(...),
    data_files: List[UploadFile] = File(...),
    report_date: str = Form(...),
    service: AgingReportService = Depends(get_aging_report_service)
):
    """
    Process aging report files and return combined report
    
    Args:
        mapping_file: CSV file containing mapping tables
        data_files: List of CSV files containing daily sales data with state info in filenames
        report_date: Specific date to use for report calculations (format: YYYY-MM-DD)
        service: AgingReportService instance
        
    Returns:
        Processed Excel file as a downloadable response or error details
    """
    try:
        logger.info(f"Processing aging report with {len(data_files)} data files")
        logger.info(f"Report date received: {report_date}")

        if not mapping_file.filename or not data_files or len(data_files) == 0:
            raise HTTPException(status_code=400, detail="Mapping file and data files are required")
        
        # Convert UploadFile objects to FileModel instances
        mapping_file_model = FileModel(
            name=mapping_file.filename,
            content=await mapping_file.read()
        )

        try:
            parsed_date = datetime.strptime(report_date, "%Y-%m-%d")
            logger.info(f"Using specified report date: {parsed_date}")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid report_date format. Expected YYYY-MM-DD")
        
        data_file_models = []
        for file in data_files:
            if not file.filename:
                raise HTTPException(status_code=400, detail="Data file is required")
            
            data_file_models.append(
                FileModel(
                    name=file.filename,
                    content=await file.read()
                )
            )
        
        # Process the files with report date
        response: ResponseBase = await service.process_uploaded_file(
            mapping_file=mapping_file_model,
            data_files=data_file_models,
            report_date=parsed_date
        )
        
        # If processing failed, return the error response
        if not response.is_success:
            logger.error(f"Failed to process aging report: {response.errors}")
            return JSONResponse(
                status_code=400,
                content={
                    "is_success": False,
                    "errors": response.errors,
                    "data": None
                }
            )
        
        # If successful, return the file as a downloadable response
        result_file: FileModel = response.data
        
        return StreamingResponse(
            io.BytesIO(result_file.content),
            media_type="application/zip",
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



