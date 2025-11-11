from fastapi import APIRouter
from fastapi import UploadFile, File, HTTPException, Depends
from fastapi.responses import StreamingResponse, JSONResponse
import io

from models.response_base import ResponseBase
from models.file_model import FileModel
from services.bank_statement_service import BankStatementService
from services.multi_logging import LoggingService

bank_statement_router = APIRouter(
    prefix="/bank-statement",
    tags=["bank-statement"],
)

# Initialize logger
logger = LoggingService().get_logger(__name__)

# Initialize service
bank_statement_service = BankStatementService()


def get_bank_statement_service():
    """Dependency to get the bank statement service instance"""
    return bank_statement_service


@bank_statement_router.post("/process")
async def process_bank_statement(
    csv_file: UploadFile = File(...),
    service: BankStatementService = Depends(get_bank_statement_service)
):
    """
    Process Bank Statement CSV file and return ZIP with Excel files
    
    Args:
        csv_file: CSV file (.csv) containing bank transaction data
        service: BankStatementService instance
        
    Returns:
        ZIP file containing individual Excel files per account/date and summary files or error details
    """
    try:
        logger.info(f"Processing bank statement file: {csv_file.filename}")

        if not csv_file.filename:
            raise HTTPException(status_code=400, detail="CSV file is required")
        
        # Validate file extension
        if not csv_file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="File must be a CSV file (.csv)")
        
        # Convert UploadFile to FileModel
        csv_file_model = FileModel(
            name=csv_file.filename,
            content=await csv_file.read()
        )
        
        # Process the file
        response: ResponseBase = await service.process_uploaded_file(csv_file_model)
        
        # If processing failed, return the error response
        if not response.is_success:
            logger.error(f"Failed to process bank statement: {response.errors}")
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
        logger.error(f"Error in process_bank_statement endpoint: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while processing the bank statement: {str(e)}"
        )


