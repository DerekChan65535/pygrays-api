from fastapi import UploadFile, HTTPException

from services.multi_logging import LoggingService


class AgingReportService:
    def __init__(self, logger: LoggingService):
        self.logger = logger
        
    async def process_uploaded_file(self, file: UploadFile):
        self.logger.info(f"Processing file: {file.filename}, content_type: {file.content_type}")
        # This is a placeholder that throws a not implemented exception as requested
        self.logger.error("File processing not implemented yet")
        raise HTTPException(
            status_code=501,
            detail="File processing not implemented yet"
        )
