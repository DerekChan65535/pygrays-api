from fastapi import FastAPI
from services.aging_report import router as aging_report_router
from containers import AppContainer
from dependency_injector.wiring import wire

# Create and configure the dependency injection container
container = AppContainer()

# Wire the container to modules that use Provide markers
wire(
    modules=[
        "services.aging_report",
    ],
    container=container,
)

app = FastAPI(
    title="PyGrays API",
    description="API for handling aging reports",
    version="0.1.0",
)

app.include_router(aging_report_router)

# Provide the container instance to the FastAPI app
app.container = container