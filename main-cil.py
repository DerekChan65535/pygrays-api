import asyncio

from containers import RootContainer
from models.file_model import FileModel
from services.aging_report_service import AgingReportService
from utils.excel_utilities import ExcelUtilities


async def main():
    container = RootContainer()

    aging_service: AgingReportService = container.aging_report_service()

    with open(r"Z:\Ariel\ageing\mapping.csv", "rb") as f:
        mapping_file = FileModel("mapping.csv", f.read())

    with open(r"Z:\Ariel\ageing\nre raw data Sales Aged Balance - 20250416.csv", "rb") as f:
        raw_sales_data = FileModel("Sales Aged Balance NSW.csv", f.read())

    response = await aging_service.process_uploaded_file(mapping_file, [raw_sales_data])

    # You can do something with the response here, e.g., print(response)
    pass


if __name__ == '__main__':
    asyncio.run(main())
