import csv
import io
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
    def _load_csv_from_bytes(csv_data: bytes) -> list[list[str]]:
        bio = io.BytesIO(csv_data)
        
        # Try different encodings with BOM handling
        encodings_to_try = [
            ('utf-8-sig', 'UTF-8 with BOM handling'),
            ('utf-8', 'standard UTF-8'),
            ('latin-1', 'fallback encoding')
        ]
        
        last_error = None
        for encoding, desc in encodings_to_try:
            bio.seek(0)
            try:
                logger.debug(f"Trying to decode with {desc}")
                text_wrapper = io.TextIOWrapper(bio, encoding=encoding, newline='')
                sample = text_wrapper.read(4096)
                if not sample:
                    logger.debug(f"Empty sample with {desc}, trying next encoding")
                    continue
                    
                text_wrapper.seek(0)
                
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',\t')
                    logger.debug(f"Successfully sniffed CSV dialect with {desc}")
                except csv.Error:
                    logger.debug(f"Could not determine dialect with {desc}, using excel dialect")
                    dialect = csv.excel
    
                reader = csv.reader(text_wrapper, dialect=dialect)
                rows = list(reader)
                if rows:
                    logger.debug(f"Successfully read {len(rows)} rows with {desc}")
                    return rows
                logger.debug(f"No rows read with {desc}, trying next encoding")
            except Exception as e:
                last_error = e
                logger.debug(f"Error with {desc}: {str(e)}")
                continue
        
        # If we get here, all encoding attempts failed
        error_msg = f"Failed to decode CSV data with any encoding: {str(last_error)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

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

    def _load_uom_mapping_from_csv(self, csv_file: FileModel, response: ResponseBase) -> dict[str, str] | None:
        """
        Load Item Number to UOM mapping from CSV file.
        
        Args:
            csv_file: The CSV file containing item number and UOM mappings
            response: Response object to populate in case of errors
            
        Returns:
            A dictionary mapping item numbers to UOMs, or None if validation fails
        """
        logger.info("Loading UOM mapping from CSV")
        
        # Load CSV data
        soh_rows = self._load_csv_from_bytes(csv_file.content)
        
        # Skip header row if it exists
        data_rows = soh_rows[1:] if len(soh_rows) > 1 else []
        
        # Step 1: First collect all mappings as tuples
        mapping_tuples = []
        for row in data_rows:
            # Assuming item number is in the first column and UOM in the second column
            if len(row) >= 2:
                item_number = row[0].strip()
                uom = row[1].strip()
                if item_number and uom:
                    mapping_tuples.append((item_number, uom))
        
        logger.info(f"Extracted {len(mapping_tuples)} item number to UOM mappings from CSV")
        
        # Step 2: Check for duplicates with different values
        item_uom_map = {}
        conflicting_items = {}
        
        for item_number, uom in mapping_tuples:
            if item_number in item_uom_map:
                current_uom = item_uom_map[item_number]
                if current_uom != uom:
                    # Found a duplicate with different UOM value
                    if item_number not in conflicting_items:
                        conflicting_items[item_number] = [current_uom]
                    conflicting_items[item_number].append(uom)
            else:
                item_uom_map[item_number] = uom
        
        # Step 3: If there are conflicts, set error and return
        if conflicting_items:
            conflict_errors = []
            for item, uoms in conflicting_items.items():
                conflict_errors.append(f"Item {item} has conflicting UOM values: {', '.join(uoms)}")
            
            error_message = "CSV contains duplicate item numbers with different UOM values"
            logger.error(error_message)
            for err in conflict_errors:
                logger.error(err)
                
            response.errors.append(error_message)
            response.errors.extend(conflict_errors)
            response.is_success = False
            return None
        
        # Step 4: Return the validated mapping
        logger.info(f"Validated {len(item_uom_map)} item number to UOM mappings")
        return item_uom_map
        
    def _process_dropship_sales(self, txt_files: list[FileModel], response: ResponseBase) -> tuple[list, list[str]] | None:
        """
        Process DropshipSales files to extract and validate data.
        
        Args:
            txt_files: List of text files to process
            response: Response object to populate in case of errors
            
        Returns:
            A tuple containing the processed data and column names, or None if validation fails
        """
        logger.info("Processing DropshipSales files")
        
        # Filter and sort DropshipSales files
        dropship_sales_files = sorted([x for x in txt_files 
                                       if re.match(r'^DropshipSales\d{8}\.txt$', x.name)], 
                                       key=lambda x: x.name)
        
        # Validate file names
        if not self._validate_file_names_date([x.name for x in dropship_sales_files]):
            response.errors.append("Invalid file names")
            response.is_success = False
            return None
            
        # Define required columns
        required_col = ["Customer", "AX_ProductCode", "GST", "Units", "Price", "Amount", "SaleNo", "VendorNo", "ItemNo",
                       "Description", "Serial_No", "Vendor_Ref_No", "DropShipper", "Consignment", "DealNo", "Column1", "BP",
                       "SaleType", "FreightCodeDescription"]
                       
        # Parse files and combine the rows
        parsed_files = []
        for file in dropship_sales_files:
            rows = self._load_csv_from_bytes(file.content)
            parsed_files.extend(rows)
            
        logger.info(f"Loaded {len(parsed_files)} rows from {len(dropship_sales_files)} DropshipSales files")
        
        # Check if all required columns are present in the parsed data
        if parsed_files:
            # Get the first row to check column presence
            first_row = parsed_files[0] if parsed_files else []
            
            # Generate header index map if first row contains headers
            # This assumes the first row contains headers matching the required columns
            header_map = {}
            if first_row:
                for i, header in enumerate(first_row):
                    header_map[header] = i
                    
            # Check for missing columns
            missing_columns = [col for col in required_col if col not in header_map]
            
            if missing_columns:
                error_message = f"Missing required columns in data: {', '.join(missing_columns)}"
                logger.error(error_message)
                response.errors.append(error_message)
                response.is_success = False
                return None
        
        # Extract data from parsed files based on required columns
        data = []
        # Skip the header row (first row)
        for row in parsed_files[1:]:
            row_data = []
            for col in required_col:
                # Get the index for this column from our header map, default to -1 if not found
                col_index = header_map.get(col, -1)
                # If we have a valid index and the row has enough elements, get the value
                value = row[col_index] if col_index >= 0 and col_index < len(row) else ""
                
                if col in ["Amount", "Price"] and value:
                    try:
                        value = decimal.Decimal(value)
                    except decimal.InvalidOperation:
                        value = ""
                row_data.append(value)
            data.append(row_data)
            
        logger.info(f"Processed {len(data)} rows of data")
        return data, required_col
        
    def process_inventory_request(self, txt_files: list[FileModel], csv_file: FileModel) -> ResponseBase:
        logger.info("Processing inventory request")
    
        response = ResponseBase()
    
        # Load and validate UOM mapping from CSV
        unit_with_cost = self._load_uom_mapping_from_csv(csv_file, response)
        
        # If loading failed, return the response with errors
        if unit_with_cost is None:
            return response
    
        # Create an empty excel workbook
        new_workbook = Workbook()
        
        # Create a new sheet called "Dropship Sale"
        dropship_sales_sheet = new_workbook.create_sheet("Dropship Sales")
        
        # Remove the default sheet
        new_workbook.remove(new_workbook.active)
        
        # Process DropshipSales files
        dropship_result = self._process_dropship_sales(txt_files, response)
        
        # If processing failed, return the response with errors
        if dropship_result is None:
            return response
            
        data, required_col = dropship_result
        
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
