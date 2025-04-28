import logging
import re
from http.client import responses
from io import BytesIO
from typing import List

from openpyxl import Workbook

from models.response_base import ResponseBase
from models.file_model import FileModel

import decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InventoryService:

    def __init__(self):
        pass

    @staticmethod
    def _decode_file_content(content: bytes) -> str:
        return content.decode('utf-8')

    @staticmethod
    def _parse_tsv_content(content: str) -> list[dict]:
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

    @staticmethod
    def _validate_file_names_date(file_names: List[str]) -> bool:

        consensus_month = None
        consensus_year = None

        for file_name in file_names:
            # decompose file name
            # E.g., DropshipSales20250228.txt will be decomposed into Category: DropshipSales, Date: 20250228, Extension: txt

            pattern = r'^(.+)(\d{8})\.([a-zA-Z]+)$'
            match = re.match(pattern, file_name)
            if not match:
                return False

            category = match.group(1)  # Group 1: Category
            date = match.group(2)  # Group 2: Date
            extension = match.group(3)  # Group 3: Extension

            # Get month and year from date
            month = int(date[:2])
            year = int(date[2:4])


            # Ensure that all files have the same month and year
            if consensus_month is None:
                consensus_month = month
            else:
                if month != consensus_month:
                    return False

            if consensus_year is None:
                consensus_year = year
            else:
                if year != consensus_year:
                    return False



        return True

    def process_inventory_request(self, txt_files: list[FileModel]  , csv_file : FileModel) -> ResponseBase:
        logger.info("Processing inventory request")

        response = ResponseBase()

        # validate txt file names
        if not self._validate_file_names_date([x.name for x in txt_files]):
            response.errors.append("Invalid file names")
            response.is_success = False
            return response

        #Create a empty excel workbook
        new_workbook = Workbook()

        #Create a new sheet called "Dropship Sale"
        dropship_sales_sheet = new_workbook.create_sheet("Dropship Sales")

        #Remove the default sheet
        new_workbook.remove(new_workbook.active)

        dropship_sales_files = sorted([x for x in txt_files if re.match(r'^DropshipSales\d{8}\.txt$', x.name)], key=lambda x: x.name)

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
            row_data = []
            for col in required_col:
                value = row.get(col, "")
                if col in ["Amount", "Price"] and value:
                    try:
                        value = decimal.Decimal(value)
                    except decimal.InvalidOperation:
                        value = ""
                row_data.append(value)
            data.append(row_data)

        # Write data to excel sheet
        dropship_sales_sheet.append(required_col)
        for row in data:
            dropship_sales_sheet.append(row)


        # Save workbook to bytes
        workbook_bytes = BytesIO()
        new_workbook.save(workbook_bytes)
        workbook_binary = workbook_bytes.getvalue()
        response.data = FileModel(name="dropship_sales.xlsx", content=workbook_binary)

        return response
