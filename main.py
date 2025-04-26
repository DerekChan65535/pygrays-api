from fastapi import FastAPI

from containers import RootContainer
from routes.routes import api_router


def create_app():
    container = RootContainer()

    a = FastAPI()
    a.container = container
    a.include_router(api_router)
    return a

app = create_app()

