from typing import Annotated

from dependency_injector.wiring import inject, Provide
from fastapi import APIRouter, UploadFile, File
from fastapi.params import Depends
from starlette.responses import HTMLResponse

from containers import RootContainer
from services.aging_report_service import AgingReportService

aging_reports_router = APIRouter(
    prefix="/aging-reports",
    tags=["aging-reports"],
)


@aging_reports_router.post("/process-file")
@inject
async def process_file(
        file: Annotated[bytes, File()],
        service: AgingReportService = Depends(Provide[RootContainer.aging_report_service])
):
    return await service.process_uploaded_file(file)


@aging_reports_router.post("/files/")
async def create_files(files: Annotated[list[bytes], File()]):
    return {"file_sizes": [len(file) for file in files]}


@aging_reports_router.post("/uploadfiles/")
async def create_upload_files(
        files: Annotated[list[UploadFile], File(description="Multiple files as UploadFile")],
):
    return {"filenames": [file.filename for file in files]}


@aging_reports_router.get("/")
async def main():
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
        </body>
    """
    return HTMLResponse(content=content)
