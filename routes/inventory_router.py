from typing import Annotated

import fastapi.responses
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, UploadFile, File, Depends
from starlette.responses import HTMLResponse

from containers import RootContainer
from models.file_model import FileModel
from services.inventory_service import InventoryService

inventory_router = APIRouter(
    prefix="/inventory",
    tags=["inventory"]
)


@inventory_router.get("/")
async def content_html():
    content = """
        <body>
        <form action="/inventory/uploadfiles/" enctype="multipart/form-data" method="post">
        <input name="files" type="file" multiple>
        <input type="submit">
        </form>
        </body>
    """
    return HTMLResponse(content=content)


@inventory_router.post("/uploadfiles/")
@inject
async def create_upload_files(
        files: Annotated[list[UploadFile], File(description="Multiple files as UploadFile")],
        service: Annotated[InventoryService, Depends(Provide[RootContainer.inventory_service])]
):
    if not files:
        return {"error": "No files uploaded"}

    first_file = files[0]
    file_content = await first_file.read()

    file_name_content = [FileModel(name=x.filename, content=await x.read()) for x in files]

    file_content=    service.process_inventory_request(file_name_content)



    return fastapi.responses.Response(
        content=file_content,
        media_type=first_file.content_type,
        headers={
            "Content-Disposition": f"attachment; filename=new_book.xlsx"
        }
    )
