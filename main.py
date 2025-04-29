from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from containers import RootContainer
from routes.api_routes import api_router


def create_app():
    container = RootContainer()

    a = FastAPI()
    
    # Configure CORS middleware
    a.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
        expose_headers=["Content-Disposition"],
    )
    
    a.container = container
    a.include_router(api_router)
    return a

app = create_app()