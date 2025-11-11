from fastapi import APIRouter
from fastapi import UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
import io

from models.response_base import ResponseBase
from models.file_model import FileModel
from services.payment_extract_service import PaymentExtractService
from services.multi_logging import LoggingService

payment_extract_router = APIRouter(
    prefix="/payment-extract",
    tags=["payment-extract"],
)

# Initialize logger
logger = LoggingService().get_logger(__name__)

# Initialize service
payment_extract_service = PaymentExtractService()


def get_payment_extract_service():
    """Dependency to get the payment extract service instance"""
    return payment_extract_service


@payment_extract_router.post("/process")
async def process_payment_extract(
    excel_file: UploadFile = File(...),
    service: PaymentExtractService = Depends(get_payment_extract_service)
):
    """
    Process Payment Extract Excel file and return ZIP with split files
    
    Args:
        excel_file: Excel file (.xls or .xlsx) containing Payments Extract sheet
        service: PaymentExtractService instance
        
    Returns:
        ZIP file containing individual Excel files for each BusinessEntity or error details
    """
    try:
        logger.info(f"Processing payment extract file: {excel_file.filename}")

        if not excel_file.filename:
            raise HTTPException(status_code=400, detail="Excel file is required")
        
        # Validate file extension - accept both .xls and .xlsx
        filename_lower = excel_file.filename.lower()
        if not (filename_lower.endswith('.xlsx') or filename_lower.endswith('.xls')):
            raise HTTPException(status_code=400, detail="File must be an Excel file (.xls or .xlsx)")
        
        # Convert UploadFile to FileModel
        excel_file_model = FileModel(
            name=excel_file.filename,
            content=await excel_file.read()
        )
        
        # Process the file
        response: ResponseBase = await service.process_uploaded_file(excel_file_model)
        
        # If processing failed, return the error response
        if not response.is_success:
            logger.error(f"Failed to process payment extract: {response.errors}")
            return JSONResponse(
                status_code=400,
                content={
                    "is_success": False,
                    "errors": response.errors,
                    "data": None
                }
            )
        
        # If successful, return the ZIP file as a downloadable response
        result_file: FileModel = response.data
        
        return StreamingResponse(
            io.BytesIO(result_file.content),
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={result_file.name}"
            }
        )
        
    except Exception as e:
        logger.error(f"Error in process_payment_extract endpoint: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while processing the payment extract: {str(e)}"
        )

