from fastapi import FastAPI
from dependency_injector.wiring import wire, inject

from containers import RootContainer
from routes.aging_report_router import aging_reports_router


def create_app():
    container = RootContainer()

    a = FastAPI()
    a.container = container
    a.include_router(aging_reports_router)
    return a

app = create_app()

