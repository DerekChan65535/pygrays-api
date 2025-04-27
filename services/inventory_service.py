import logging
import re
from io import BytesIO

from openpyxl import Workbook

from models.file_model import FileModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InventoryService:

    def __init__(self):
        pass

    def _decode_file_content(self, content: bytes) -> str:
        return content.decode('utf-8')

    def _parse_tsv_content(self, content: str) -> list[dict]:
        rows = []
        lines = content.splitlines()
        if not lines:
            return rows

        headers = lines[0].split('\t')
        for line in lines[1:]:
            values = line.split('\t')
            row = dict(zip(headers, values))
            rows.append(row)
        return rows

    def process_inventory_request(self, files: list[FileModel]) -> bytes:
        logger.info("Processing inventory request")

        #Create a empty excel workbook
        new_workbook = Workbook()

        #Create a new sheet called "Dropship Sale"
        dropship_sales_sheet = new_workbook.create_sheet("Dropship Sales")

        #Remove the default sheet
        new_workbook.remove(new_workbook.active)

        dropship_sales_files = sorted([x for x in files if re.match(r'^DropshipSales\d{8}\.txt$', x.name)], key=lambda x: x.name)

        required_col = ["Customer", "AX_ProductCode", "GST", "Units", "Price", "Amount", "SaleNo", "VendorNo", "ItemNo",
                   "Description", "Serial_No", "Vendor_Ref_No", "DropShipper", "Consignment", "DealNo", "Column1", "BP",
                   "SaleType", "FreightCodeDescription"]
        parsed_files = []
        for file in dropship_sales_files:
            content = self._decode_file_content(file.content)
            rows = self._parse_tsv_content(content)
            parsed_files.extend(rows)

        # extract data from parsed files by `required_col`
        data = []
        for row in parsed_files:
            data.append([row.get(col, "") for col in required_col])

        # Write data to excel sheet
        dropship_sales_sheet.append(required_col)
        for row in data:
            dropship_sales_sheet.append(row)


        # Save workbook to bytes
        workbook_bytes = BytesIO()
        new_workbook.save(workbook_bytes)
        workbook_binary = workbook_bytes.getvalue()

        return workbook_binary
