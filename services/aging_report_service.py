from fastapi import UploadFile, HTTPException

from services.multi_logging import LoggingService


class AgingReportService:
    def __init__(self):
        pass

    async def process_uploaded_file(self, file: UploadFile):
        # This is a placeholder that throws a not implemented exception as requested
        raise HTTPException(
            status_code=501,
            detail="File processing not implemented yet"
        )
