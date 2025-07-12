from typing import Annotated
import traceback
import json

import fastapi.responses
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, UploadFile, File, Depends
from starlette.responses import HTMLResponse, JSONResponse

from containers import RootContainer
from models.file_model import FileModel
from services.inventory_service import InventoryService

inventory_router = APIRouter(
    prefix="/inventory",
    tags=["inventory"]
)


@inventory_router.post("/uploadfiles/")
@inject
async def create_upload_files(
        txt_files: Annotated[list[UploadFile], File(description="Text files as UploadFile")],
        csv_files: Annotated[list[UploadFile], File(description="SOH CSV files as UploadFile")],
        service: Annotated[InventoryService, Depends(Provide[RootContainer.inventory_service])]
):
    if not txt_files or len(txt_files) == 0:
        return fastapi.responses.Response(
            status_code=400,
            content="No txt files uploaded"
        )

    if not csv_files or len(csv_files) == 0:
        return fastapi.responses.Response(
            status_code=400,
            content="No SOH CSV files uploaded"
        )

    if any(not x.filename for x in txt_files):
        return fastapi.responses.Response(
            status_code=400,
            content="One or more txt files have no filename"
        )
    
    # Validate CSV filenames
    invalid_csv_files = []
    for csv_file in csv_files:
        if not csv_file.filename:
            return fastapi.responses.Response(
                status_code=400,
                content="One or more CSV files have no filename"
            )
        
        # Check if filename ends with DDMMYY pattern
        filename = csv_file.filename
        name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
        if len(name_without_ext) < 6 or not name_without_ext[-6:].isdigit():
            invalid_csv_files.append(filename)
    
    if invalid_csv_files:
        return fastapi.responses.Response(
            status_code=400,
            content=f"Invalid SOH filename(s) - must end with DDMMYY pattern: {', '.join(invalid_csv_files)}"
        )

    txt_file_name_content = [FileModel(x.filename, await x.read()) for x in txt_files]
    csv_file_name_content = [FileModel(x.filename, await x.read()) for x in csv_files]

    try:
        response = service.process_inventory_request(txt_file_name_content, csv_file_name_content)
    except Exception as e:
        tb_exc = traceback.TracebackException.from_exception(e)
        content = {
            "exception": {
                "type": tb_exc.exc_type.__name__,
                "message": str(e)
            },
            "traceback": [
                {
                    "filename": frame.filename,
                    "lineno": frame.lineno,
                    "name": frame.name,
                    "line": frame.line
                } for frame in tb_exc.stack
            ]
        }
        # Use JSONResponse for dictionary content
        return JSONResponse(
            status_code=500,
            content=content
        )

    if response.is_success:
        # Assuming response.data contains the file bytes
        # You might want to explicitly set the media_type if it's not plain text
        # e.g., media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' for xlsx
        return fastapi.responses.Response(
            status_code=200,
            content=response.data.content,
            headers={
                "Content-Disposition": f"attachment; filename={response.data.name}"
                # Make sure response.data has a 'name' attribute if used like this
            }
            # Consider adding media_type=... here based on the actual file type in response.data
        )
    else:
        # If response.to_json() returns a dictionary/list, use JSONResponse
        # If it returns a JSON string, Response is okay, but set media_type
        try:
            # Try to parse the JSON string potentially returned by to_json()
            return JSONResponse(
                status_code=400,
                content=response.to_dict()
            )
        except (json.JSONDecodeError, TypeError):
            # If to_json() returns a plain string or something else, use Response
            return fastapi.responses.Response(
                status_code=400,
                content=str(response.to_dict()),  # Ensure it's a string
                media_type="application/json"  # Assuming the string is JSON
            )
