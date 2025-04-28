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
        txt_files: Annotated[list[UploadFile], File(description="Text files as UploadFile")],
        csv_files: Annotated[list[UploadFile], File(description="CSV files as UploadFile")],
        service: Annotated[InventoryService, Depends(Provide[RootContainer.inventory_service])]
):
    if not txt_files or len(txt_files) == 0:
        return fastapi.responses.Response(
            status_code=400,
            content="No txt files uploaded"
        )

    if not csv_files or len(csv_files) != 1:
        return fastapi.responses.Response(
            status_code=400,
            content="No csv files uploaded"
        )

    txt_file_name_content = [FileModel(x.filename, await x.read()) for x in txt_files]
    csv_uom_file = FileModel(csv_files[0].filename, await csv_files[0].read())

    response = service.process_inventory_request(txt_file_name_content, csv_uom_file)


    if response.is_success:
        return fastapi.responses.Response(
            status_code=200,
            content=response.data
        )
    else:
        return fastapi.responses.Response(
            status_code=400,
            content=response.to_json()
        )



    # return fastapi.responses.Response(
    #     content=file_content,
    #     media_type=first_file.content_type,
    #     headers={
    #         "Content-Disposition": f"attachment; filename=new_book.xlsx"
    #     }
    # )
