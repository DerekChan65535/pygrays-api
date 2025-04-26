from typing import Annotated

import fastapi.responses
from fastapi import APIRouter, UploadFile, File
from starlette.responses import HTMLResponse

inventory_router = APIRouter(
    prefix="/inventory",
    tags=["inventory"]
)


@inventory_router.get("/")
async def content_html():
    content = """
        <body>
        <form action="/aging-reports/uploadfiles/" enctype="multipart/form-data" method="post">
        <input name="files" type="file" multiple>
        <input type="submit">
        </form>
        </body>
    """
    return HTMLResponse(content=content)


@inventory_router.post("/uploadfiles/")
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
