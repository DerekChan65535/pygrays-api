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
    if response.is_success and response.data:
        output_filename = response.data.name
        output_path = fr"Z:\Ariel\ageing\{output_filename}"  # Use raw string for Windows path
        try:
            with open(output_path, "wb") as f:
                f.write(response.data.content)
            print(f"File saved successfully to: {output_path}")
        except Exception as e:
            print(f"Error saving file: {e}")
    elif not response.is_success:
        print(f"Processing failed: {response.errors}")
    else:
        print("Processing completed but no data to save.")


if __name__ == '__main__':
    asyncio.run(main())
