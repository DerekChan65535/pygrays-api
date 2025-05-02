import csv
import io
import logging
import re
from typing import List, Tuple, Dict, Optional

from openpyxl import Workbook
from models.response_base import ResponseBase
from models.file_model import FileModel

import decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InventoryService:
    # Define required columns with their expected data types
    dropship_sales_import_schema = {
        "Customer": str,
        "AX_ProductCode": str,
        "GST": str,
        "Units": int,
        "Price": decimal.Decimal,
        "Amount": decimal.Decimal,
        "SaleNo": str,
        "VendorNo": str,
        "ItemNo": str,
        "Description": str,
        "Serial_No": str,
        "Vendor_Ref_No": str,
        "DropShipper": str,
        "Consignment": str,
        "DealNo": str,
        "Column1": str,
        "BP": decimal.Decimal,
        "SaleType": str,
        "FreightCodeDescription": str
    }

    deals_import_schema = {
        "Customer": str,
        "AX_ProductCode": str,
        "GST": str,
        "Units": int,
        "Price": decimal.Decimal,
        "Amount": decimal.Decimal,
        "SaleNo": str,
        "VendorNo": str,
        "ItemNo": str,
        "Description": str,
        "Serial_No": str,
        "Vendor_Ref_No": str,
        "DropShipper": str,
        "Consignment": str,
        "DealNo": str,
        "Column1": str,
        "BP": decimal.Decimal,
        "SaleType": str,
        "DivisionCode": str,
        "DivisionDescription": str,
        "FreightCodeDescription": str
    }

    # Note: uom_columns_schema is defined but not fully used in the original code.
    # For mapping, we only need "Item" and "UOM", so we'll define a specific schema for that.
    uom_mapping_schema = {
        "Item": str,
        "UOM": decimal.Decimal
    }

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
    def _extract_and_validate_file_dates(file_names: List[str]) -> Tuple[Optional[int], Optional[int], bool]:
        """
        Extract month and year from file names and validate they are consistent.
        
        Args:
            file_names: List of file names to process.
            
        Returns:
            Tuple containing:
            - Extracted month (or None if invalid)
            - Extracted year (or None if invalid)
            - Boolean indicating if validation was successful
        """
        consensus_month = None
        consensus_year = None
        
        if not file_names:
            return None, None, False
    
        for file_name in file_names:
            # decompose file name
            # E.g., DropshipSales20250228.txt will be decomposed into Category: DropshipSales, Date: 20250228, Extension: txt
    
            pattern = r'^(.+)(\d{8})\.([a-zA-Z]+)$'
            match = re.match(pattern, file_name)
            if not match:
                return None, None, False
    
            category = match.group(1)  # Group 1: Category
            date = match.group(2)  # Group 2: Date
            extension = match.group(3)  # Group 3: Extension
    
            # Get month and year from date
            month = int(date[4:6])
            year = int(date[0:4])
            
            # Validate month range
            if month < 1 or month > 12:
                return None, None, False
    
            # Ensure that all files have the same month and year
            if consensus_month is None:
                consensus_month = month
            else:
                if month != consensus_month:
                    return consensus_month, consensus_year, False
    
            if consensus_year is None:
                consensus_year = year
            else:
                if year != consensus_year:
                    return consensus_month, consensus_year, False
    
        return consensus_month, consensus_year, True
        
    def _get_mixed_deals(self, data_dicts: List[Dict]) -> List[Dict]:
        """
        Extract records with MIXED deal numbers from the dataset.
        
        Args:
            data_dicts: List of data dictionaries to filter.
            
        Returns:
            List of dictionaries with MIXED deal numbers.
        """
        mixed_deals = [item for item in data_dicts if str(item.get("DealNo", "")).upper() == "MIXED"]
        logger.info(f"Found {len(mixed_deals)} rows with MIXED DealNo")
        return mixed_deals
        
    def _handle_errors(self, errors: List[str], response: ResponseBase) -> bool:
        """
        Helper method to check for errors and update response accordingly.
        
        Args:
            errors: List of error messages to check.
            response: Response object to update if errors exist.
            
        Returns:
            True if errors exist, False otherwise.
        """
        if len(errors) > 0:
            response.errors.extend(errors)
            response.is_success = False
            return True
        return False
        
    def _prepare_workbook(self, errors: List[str]) -> Optional[Workbook]:
        """
        Create and prepare a new Excel workbook with necessary sheets.
        
        Args:
            errors: List to collect error messages.
            
        Returns:
            Prepared Workbook object or None if error occurs.
        """
        try:
            new_workbook = Workbook()
            
            # Create sheets for data
            dropship_sales_sheet = new_workbook.create_sheet("Dropship Sales")
            mixed_sheet = new_workbook.create_sheet("Mixed")
            wine_sheet = new_workbook.create_sheet("Wine")
            
            # Remove the default sheet
            new_workbook.remove(new_workbook.active)
            
            return new_workbook
        except Exception as e:
            errors.append(f"Error creating workbook: {str(e)}")
            logger.error("Error creating workbook", exc_info=True)
            return None
            
    def _write_dropship_sales_sheet(self, workbook: Workbook, data_dicts: List[Dict], errors: List[str]) -> bool:
        """
        Write dropship sales data to the Excel sheet.
        
        Args:
            workbook: The workbook to write to.
            data_dicts: Data to write to the sheet.
            errors: List to collect error messages.
            
        Returns:
            True if successful, False if errors occurred.
        """
        try:
            sheet = workbook["Dropship Sales"]
            
            # Get column names in consistent order
            required_col_names = list(self.dropship_sales_import_schema.keys())
            
            # Write header row 
            sheet.append(required_col_names)
            
            # Write data rows using the required column order
            for item in data_dicts:
                row_values = [item.get(col, "") for col in required_col_names]
                sheet.append(row_values)
                
            logger.info(f"Wrote {len(data_dicts)} rows to Dropship Sales sheet")
            return True
        except Exception as e:
            errors.append(f"Error writing to Dropship Sales sheet: {str(e)}")
            logger.error("Error writing to Dropship Sales sheet", exc_info=True)
            return False
            
    def _write_mixed_sheet(self, workbook: Workbook, mixed_deals: List[Dict], 
                          unit_with_cost: Dict[str, str], errors: List[str]) -> bool:
        """
        Write mixed deals data to the Mixed sheet with Per_Unit_Cost included.
        Also calculates and includes COGS, SALE_EX_GST, and BP_EX_GST columns.
        
        Args:
            workbook: The workbook to write to.
            mixed_deals: Mixed deals data to write.
            unit_with_cost: Mapping of product codes to unit costs.
            errors: List to collect error messages.
            
        Returns:
            True if successful, False if errors occurred.
        """
        try:
            sheet = workbook["Mixed"]
            
            # Get column names in consistent order
            required_col_names = list(self.dropship_sales_import_schema.keys())
            
            # Create mixed sheet header with Per_Unit_Cost column after AX_ProductCode
            # and COGS, SALE_EX_GST, BP_EX_GST columns after Serial_No
            mixed_sheet_headers = []
            for col in required_col_names:
                mixed_sheet_headers.append(col)
                if col == "AX_ProductCode":
                    mixed_sheet_headers.append("Per_Unit_Cost")
                elif col == "Serial_No":
                    mixed_sheet_headers.append("COGS")
                    mixed_sheet_headers.append("SALE_EX_GST")
                    mixed_sheet_headers.append("BP_EX_GST")
            
            # Write header to mixed sheet
            sheet.append(mixed_sheet_headers)
    
            # Write mixed deals data to the mixed sheet with Per_Unit_Cost and calculated columns
            for item in mixed_deals:
                row_values = []
                per_unit_cost_value = None
                
                for col in required_col_names:
                    row_values.append(item.get(col, ""))
                    
                    if col == "AX_ProductCode":
                        # Add Per_Unit_Cost value from unit_with_cost if available
                        product_code = item.get("AX_ProductCode", "")
                        per_unit_cost = unit_with_cost.get(product_code, "")
                        per_unit_cost_value = per_unit_cost
                        # Round per_unit_cost to 2 decimal places for display in Excel, without affecting calculation value
                        display_per_unit_cost = ""
                        if per_unit_cost:
                            try:
                                per_unit_cost_decimal = decimal.Decimal(per_unit_cost) if per_unit_cost else decimal.Decimal('0')
                                display_per_unit_cost = per_unit_cost_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                            except (decimal.InvalidOperation, TypeError):
                                display_per_unit_cost = per_unit_cost
                        row_values.append(display_per_unit_cost)
                        
                    elif col == "Serial_No":
                        # Calculate and add COGS = Per_Unit_Cost * Units (rounded to 2 decimal places)
                        units_value = item.get("Units", 0)
                        cogs_value = ""
                        if per_unit_cost_value and units_value:
                            try:
                                per_unit_cost_decimal = decimal.Decimal(per_unit_cost_value) if per_unit_cost_value else decimal.Decimal('0')
                                cogs_decimal = per_unit_cost_decimal * decimal.Decimal(units_value)
                                # Round to 2 decimal places using ROUND_HALF_UP
                                cogs_value = cogs_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                            except (decimal.InvalidOperation, TypeError):
                                cogs_value = ""
                        row_values.append(cogs_value)
                        
                        # Calculate and add SALE_EX_GST = Amount / 1.1 (rounded to 2 decimal places)
                        amount_value = item.get("Amount", "")
                        sale_ex_gst_value = ""
                        if amount_value:
                            try:
                                if isinstance(amount_value, decimal.Decimal):
                                    sale_ex_gst_decimal = amount_value / decimal.Decimal('1.1')
                                    # Round to 2 decimal places using ROUND_HALF_UP
                                    sale_ex_gst_value = sale_ex_gst_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                                else:
                                    sale_ex_gst_value = ""
                            except (decimal.InvalidOperation, TypeError):
                                sale_ex_gst_value = ""
                        row_values.append(sale_ex_gst_value)
                        
                        # Calculate and add BP_EX_GST = BP / 1.1 (rounded to 2 decimal places)
                        bp_value = item.get("BP", "")
                        bp_ex_gst_value = ""
                        if bp_value:
                            try:
                                if isinstance(bp_value, decimal.Decimal):
                                    bp_ex_gst_decimal = bp_value / decimal.Decimal('1.1')
                                    # Round to 2 decimal places using ROUND_HALF_UP
                                    bp_ex_gst_value = bp_ex_gst_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                                else:
                                    bp_ex_gst_value = ""
                            except (decimal.InvalidOperation, TypeError):
                                bp_ex_gst_value = ""
                        row_values.append(bp_ex_gst_value)
                        
                sheet.append(row_values)
                
            logger.info(f"Wrote {len(mixed_deals)} rows to Mixed sheet")
            return True
        except Exception as e:
            errors.append(f"Error writing to Mixed sheet: {str(e)}")
            logger.error("Error writing to Mixed sheet", exc_info=True)
            return False
    
    def _write_wine_sheet(self, workbook: Workbook, data_dicts: List[Dict], 
                         unit_with_cost: Dict[str, str], errors: List[str]) -> bool:
        """
        Write wine deals data to the Wine sheet with Per_Unit_Cost included.
        Also calculates and includes COGS, SALE_EX_GST, and BP_EX_GST columns.
        
        Args:
            workbook: The workbook to write to.
            data_dicts: Data to write to the sheet.
            unit_with_cost: Mapping of product codes to unit costs.
            errors: List to collect error messages.
            
        Returns:
            True if successful, False if errors occurred.
        """
        try:
            sheet = workbook["Wine"]
            
            # Get column names in consistent order
            required_col_names = list(self.deals_import_schema.keys())
            
            # Create wine sheet header with Per_Unit_Cost column after AX_ProductCode
            # and COGS, SALE_EX_GST, BP_EX_GST columns after Serial_No
            wine_sheet_headers = []
            for col in required_col_names:
                wine_sheet_headers.append(col)
                if col == "AX_ProductCode":
                    wine_sheet_headers.append("Per_Unit_Cost")
                elif col == "Serial_No":
                    wine_sheet_headers.append("COGS")
                    wine_sheet_headers.append("SALE_EX_GST")
                    wine_sheet_headers.append("BP_EX_GST")
            
            # Write header to wine sheet
            sheet.append(wine_sheet_headers)
            
            # Write data rows with Per_Unit_Cost and calculated columns
            for item in data_dicts:
                row_values = []
                per_unit_cost_value = None
                
                for col in required_col_names:
                    row_values.append(item.get(col, ""))
                    
                    if col == "AX_ProductCode":
                        # Add Per_Unit_Cost value from unit_with_cost if available
                        product_code = item.get("AX_ProductCode", "")
                        per_unit_cost = unit_with_cost.get(product_code, "")
                        per_unit_cost_value = per_unit_cost
                        # Round per_unit_cost to 2 decimal places for display in Excel, without affecting calculation value
                        display_per_unit_cost = ""
                        if per_unit_cost:
                            try:
                                per_unit_cost_decimal = decimal.Decimal(per_unit_cost) if per_unit_cost else decimal.Decimal('0')
                                display_per_unit_cost = per_unit_cost_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                            except (decimal.InvalidOperation, TypeError):
                                display_per_unit_cost = per_unit_cost
                        row_values.append(display_per_unit_cost)
                        
                    elif col == "Serial_No":
                        # Calculate and add COGS = Per_Unit_Cost * Units (rounded to 2 decimal places)
                        units_value = item.get("Units", 0)
                        cogs_value = ""
                        if per_unit_cost_value and units_value:
                            try:
                                per_unit_cost_decimal = decimal.Decimal(per_unit_cost_value) if per_unit_cost_value else decimal.Decimal('0')
                                cogs_decimal = per_unit_cost_decimal * decimal.Decimal(units_value)
                                # Round to 2 decimal places using ROUND_HALF_UP
                                cogs_value = cogs_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                            except (decimal.InvalidOperation, TypeError):
                                cogs_value = ""
                        row_values.append(cogs_value)
                        
                        # Calculate and add SALE_EX_GST = Amount / 1.1 (rounded to 2 decimal places)
                        amount_value = item.get("Amount", "")
                        sale_ex_gst_value = ""
                        if amount_value:
                            try:
                                if isinstance(amount_value, decimal.Decimal):
                                    sale_ex_gst_decimal = amount_value / decimal.Decimal('1.1')
                                    # Round to 2 decimal places using ROUND_HALF_UP
                                    sale_ex_gst_value = sale_ex_gst_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                                else:
                                    sale_ex_gst_value = ""
                            except (decimal.InvalidOperation, TypeError):
                                sale_ex_gst_value = ""
                        row_values.append(sale_ex_gst_value)
                        
                        # Calculate and add BP_EX_GST = BP / 1.1 (rounded to 2 decimal places)
                        bp_value = item.get("BP", "")
                        bp_ex_gst_value = ""
                        if bp_value:
                            try:
                                if isinstance(bp_value, decimal.Decimal):
                                    bp_ex_gst_decimal = bp_value / decimal.Decimal('1.1')
                                    # Round to 2 decimal places using ROUND_HALF_UP
                                    bp_ex_gst_value = bp_ex_gst_decimal.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                                else:
                                    bp_ex_gst_value = ""
                            except (decimal.InvalidOperation, TypeError):
                                bp_ex_gst_value = ""
                        row_values.append(bp_ex_gst_value)
                        
                sheet.append(row_values)
                
            logger.info(f"Wrote {len(data_dicts)} rows to Wine sheet")
            return True
        except Exception as e:
            errors.append(f"Error writing to Wine sheet: {str(e)}")
            logger.error("Error writing to Wine sheet", exc_info=True)
            return False

    def _load_csv_data(self, file: FileModel, schema: Dict[str, type], errors: List[str]) -> List[Dict]:
        """
        Load and validate CSV data based on a given schema.
    
        Args:
            file: The CSV file to process.
            schema: Dictionary mapping column names to expected data types.
            errors: List to collect error messages.
    
        Returns:
            List of validated row dictionaries. Empty if validation fails.
        """
        try:
            rows = self._load_csv_from_bytes(file.content)
            if not rows:
                errors.append(f"No data in file {file.name}")
                return []
    
            headers = rows[0]
            missing_columns = [col for col in schema.keys() if col not in headers]
            if missing_columns:
                error_message = f"Missing required columns in {file.name}: {', '.join(missing_columns)}"
                errors.append(error_message)
                return []
    
            validated_rows = []
    
            for row_index, row in enumerate(rows[1:], start=2):  # Skip header
                if len(row) != len(headers):
                    errors.append(f"Row {row_index} in {file.name}: mismatched number of columns")
                    continue
    
                item = {}
                row_invalid = False
                for i, header_name in enumerate(headers):
                    value = row[i] if i < len(row) else ""
                    if header_name in schema:
                        expected_type = schema[header_name]
                        if value:
                            if expected_type == decimal.Decimal:
                                try:
                                    # remove anything but digits and decimal point from the string
                                    value = re.sub(r'[^\d.]', '', value)
                                    value = decimal.Decimal(value)
                                except decimal.InvalidOperation:
                                    errors.append(f"Row {row_index}, column '{header_name}' in {file.name}: invalid decimal value '{value}'")
                                    row_invalid = True
                                    break  # Skip this row
                            elif expected_type == int:
                                try:
                                    # remove anything but digits from the string
                                    value = re.sub(r'[^\d]', '', value)
                                    value = int(value) if value else 0
                                except ValueError:
                                    errors.append(f"Row {row_index}, column '{header_name}' in {file.name}: invalid integer value '{value}'")
                                    row_invalid = True
                                    break  # Skip this row
                    item[header_name] = value
                if not row_invalid:
                    validated_rows.append(item)
    
            return validated_rows
        except Exception as e:
            errors.append(f"Error processing file {file.name}: {str(e)}")
            logger.error(f"Error processing file {file.name}", exc_info=True)
            return []

    def _load_uom_mapping_from_csv(self, csv_file: FileModel, errors: List[str]) -> Optional[Dict[str, str]]:
        """
        Load Item Number to UOM mapping from a CSV file.
    
        Args:
            csv_file: The CSV file containing item number and UOM mappings.
            errors: List to collect error messages.
    
        Returns:
            Dictionary mapping item numbers to UOMs, or None if validation fails.
        """
        logger.info("Loading UOM mapping from CSV")
    
        rows = self._load_csv_data(csv_file, self.uom_mapping_schema, errors)
        if len(errors) > 0:
            logger.error(f"Errors loading UOM mapping from {csv_file.name}: {errors}")
            return None
    
        # Build the mapping and check for conflicts
        item_uom_map = {}
        conflicting_items = {}
    
        for row in rows:
            item_number = row["Item"]
            uom = row["UOM"]
            if item_number in item_uom_map:
                if item_uom_map[item_number] != uom:
                    if item_number not in conflicting_items:
                        conflicting_items[item_number] = [item_uom_map[item_number]]
                    conflicting_items[item_number].append(uom)
            else:
                item_uom_map[item_number] = uom
    
        if len(conflicting_items) > 0:
            conflict_errors = [f"Item {item} has conflicting UOM values: {', '.join(map(str, uoms))}"
                              for item, uoms in conflicting_items.items()]
            error_message = "CSV contains duplicate item numbers with different UOM values"
            logger.error(error_message)
            for err in conflict_errors:
                logger.error(err)
            errors.append(error_message)
            errors.extend(conflict_errors)
            return None

        logger.info(f"Validated {len(item_uom_map)} item number to UOM mappings")
        return item_uom_map

    def _process_deals_files(self, txt_files: List[FileModel], errors: List[str]) -> Tuple[Optional[List[Dict]], Optional[int], Optional[int]]:
        """
        Process Deals files to extract and validate data.
    
        Args:
            txt_files: List of text files to process.
            errors: List to collect error messages.
    
        Returns:
            Tuple containing:
            - List of dictionaries with validated data, or None if validation fails
            - Month extracted from file names, or None if validation fails
            - Year extracted from file names, or None if validation fails
        """
        logger.info("Processing Deals files")
    
        deals_files = sorted([x for x in txt_files if re.match(r'^Deals\d{8}\.txt$', x.name)],
                              key=lambda x: x.name)
        
        if not deals_files:
            errors.append("No Deals files found in the provided files")
            return None, None, None
        
        # Extract and validate the month and year from file names
        month, year, is_valid = self._extract_and_validate_file_dates([x.name for x in deals_files])
        if not is_valid:
            errors.append("Invalid file names - all Deals files must have the same month and year")
            return None, None, None
    
        all_items = []
        for file in deals_files:
            items = self._load_csv_data(file, self.deals_import_schema, errors)
            if len(errors) > 0:
                logger.error(f"Errors processing {file.name}: {errors}")
                return None, None, None
            all_items.extend(items)
    
        logger.info(f"Processed {len(all_items)} rows of data from {len(deals_files)} files")
        return all_items, month, year
    
    def _process_dropship_sales_files(self, txt_files: List[FileModel], errors: List[str]) -> Tuple[Optional[List[Dict]], Optional[int], Optional[int]]:
        """
        Process DropshipSales files to extract and validate data.
    
        Args:
            txt_files: List of text files to process.
            errors: List to collect error messages.
    
        Returns:
            Tuple containing:
            - List of dictionaries with validated data, or None if validation fails
            - Month extracted from file names, or None if validation fails
            - Year extracted from file names, or None if validation fails
        """
        logger.info("Processing DropshipSales files")
    
        dropship_sales_files = sorted([x for x in txt_files if re.match(r'^DropshipSales\d{8}\.txt$', x.name)],
                                      key=lambda x: x.name)
        
        if not dropship_sales_files:
            errors.append("No DropshipSales files found in the provided files")
            return None, None, None
        
        # Extract and validate the month and year from file names
        month, year, is_valid = self._extract_and_validate_file_dates([x.name for x in dropship_sales_files])
        if not is_valid:
            errors.append("Invalid file names - all DropshipSales files must have the same month and year")
            return None, None, None
    
        all_items = []
        for file in dropship_sales_files:
            items = self._load_csv_data(file, self.dropship_sales_import_schema, errors)
            if len(errors) > 0:
                logger.error(f"Errors processing {file.name}: {errors}")
                return None, None, None
            all_items.extend(items)
    
        logger.info(f"Processed {len(all_items)} rows of data from {len(dropship_sales_files)} files")
        return all_items, month, year

    def _get_month_name(self, month: int) -> str:
        """
        Convert month number to month name.
        
        Args:
            month: Month number (1-12)
            
        Returns:
            Month name (e.g., "January", "February", etc.)
        """
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        if 1 <= month <= 12:
            return month_names[month - 1]
        return "Unknown"
    
    def process_inventory_request(self, txt_files: List[FileModel], csv_file: FileModel) -> ResponseBase:
        """
        Process inventory request by handling input files, generating data, and creating an Excel file.
        
        Args:
            txt_files: List of text files containing dropship sales and deals data.
            csv_file: CSV file containing UOM mapping data.
            
        Returns:
            Response object with success status, Excel file, and any errors.
        """
        logger.info("Processing inventory request")
        response = ResponseBase()
        errors = []  # For collecting errors from all operations
        
        # =====================================================================
        # STAGE 1: Process input files, validate columns and data
        # =====================================================================
        logger.info("Stage 1: Processing and validating input files")
        
        # Load and validate UOM mapping from CSV
        unit_with_cost = self._load_uom_mapping_from_csv(csv_file, errors)
        if self._handle_errors(errors, response):
            return response
        
        # Process DropshipSales files
        data_dicts, dropship_month, dropship_year = self._process_dropship_sales_files(txt_files, errors)
        if self._handle_errors(errors, response):
            return response
            
        # Process Deals files
        deals_data, deals_month, deals_year = self._process_deals_files(txt_files, errors)
        if self._handle_errors(errors, response):
            return response
        
        # Validate both types of files have the same month and year
        if dropship_month is not None and deals_month is not None:
            if dropship_month != deals_month or dropship_year != deals_year:
                error_msg = f"Files from different periods: DropshipSales files are from {self._get_month_name(dropship_month)} 20{dropship_year}, while Deals files are from {self._get_month_name(deals_month)} 20{deals_year}"
                errors.append(error_msg)
                logger.error(error_msg)
                self._handle_errors(errors, response)
                return response
        
        # Get month and year for file naming
        file_month = dropship_month or deals_month
        file_year = dropship_year or deals_year
        
        if file_month is None or file_year is None:
            error_msg = "Could not determine month and year from file names"
            errors.append(error_msg)
            logger.error(error_msg)
            self._handle_errors(errors, response)
            return response
            
        # =====================================================================
        # STAGE 2: Generate/calculate extra data
        # =====================================================================
        logger.info("Stage 2: Generating and calculating extra data")
        
        # Extract mixed deals for separate processing
        mixed_deals = self._get_mixed_deals(data_dicts)
    
        # =====================================================================
        # STAGE 3: Prepare Excel sheets and write data
        # =====================================================================
        logger.info("Stage 3: Preparing Excel file and writing data")
        
        # Prepare workbook with necessary sheets
        new_workbook = self._prepare_workbook(errors)
        if self._handle_errors(errors, response):
            return response
            
        # Write data to Dropship Sales sheet
        if not self._write_dropship_sales_sheet(new_workbook, data_dicts, errors):
            if self._handle_errors(errors, response):
                return response
            
        # Write mixed deals data to Mixed sheet
        if not self._write_mixed_sheet(new_workbook, mixed_deals, unit_with_cost, errors):
            if self._handle_errors(errors, response):
                return response
            
        # Write deals data to Wine sheet
        if deals_data and not self._write_wine_sheet(new_workbook, deals_data, unit_with_cost, errors):
            if self._handle_errors(errors, response):
                return response
            
        # Save the workbook and prepare response
        try:
            # Create a descriptive file name
            month_name = self._get_month_name(file_month)
            file_name = f"{month_name}_All_Sales_{file_year}.xlsx"
            
            workbook_bytes = io.BytesIO()
            new_workbook.save(workbook_bytes)
            workbook_binary = workbook_bytes.getvalue()
            
            response.data = FileModel(name=file_name, content=workbook_binary)
            logger.info(f"Excel workbook saved as {file_name} with Dropship Sales, Mixed, and Wine sheets")
            return response
        except Exception as e:
            error_msg = f"Error saving workbook: {str(e)}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)
            self._handle_errors(errors, response)
            return response