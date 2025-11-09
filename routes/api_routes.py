from fastapi import APIRouter

from routes.aging_report_router import aging_reports_router
from routes.inventory_router import inventory_router
from routes.payment_extract_router import payment_extract_router

api_router = APIRouter()

api_router.include_router(aging_reports_router)
api_router.include_router(inventory_router)
api_router.include_router(payment_extract_router)