import csv
import io
import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any, Union

import openpyxl

from models.file_model import FileModel
from models.response_base import ResponseBase

# Initialize logger with detailed configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgingReportService:
    # Define the schema for daily data import with types and formats
    daily_data_import_schema: Dict[str, Dict[str, Any]] = {
        "Classification": {"type": "string", "required": False},
        "Sale_No": {"type": "string", "required": True},
        "Description": {"type": "string", "required": False},
        "Division": {"type": "string", "required": True},
        "BDM": {"type": "string", "required": False},
        "Sale_Date": {"type": "datetime", "formats": ["%d/%m/%Y %H:%M", "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y"], "required": False},
        "Gross_Tot": {"type": "float", "required": True},
        "Delot_Ind": {"type": "boolean", "required": False},
        "Cheque_Date": {"type": "datetime", "formats": ["%d/%m/%Y %H:%M", "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y"], "required": False},
        "Day0": {"type": "float", "required": False},
        "Day1": {"type": "float", "required": False},
        "Day2": {"type": "float", "required": False},
        "Day3": {"type": "float", "required": False},
        "Day4": {"type": "float", "required": False},
        "Day5": {"type": "float", "required": False},
        "Day6": {"type": "float", "required": False},
        "Day7": {"type": "float", "required": False},
        "Day8": {"type": "float", "required": False},
        "Day9": {"type": "float", "required": False},
        "Day10": {"type": "float", "required": False},
        "Day11": {"type": "float", "required": False},
        "Day12": {"type": "float", "required": False},
        "Day13": {"type": "float", "required": False},
        "Day14": {"type": "float", "required": False},
        "Day15": {"type": "float", "required": False},
        "Day16": {"type": "float", "required": False},
        "Day17": {"type": "float", "required": False},
        "Day18": {"type": "float", "required": False},
        "Day19": {"type": "float", "required": False},
        "Day20": {"type": "float", "required": False},
        "Day21": {"type": "float", "required": False},
        "Day22": {"type": "float", "required": False},
        "Day23": {"type": "float", "required": False},
        "Day24": {"type": "float", "required": False},
        "Day25": {"type": "float", "required": False},
        "Day26": {"type": "float", "required": False},
        "Day27": {"type": "float", "required": False},
        "Day28": {"type": "float", "required": False},
        "Day29": {"type": "float", "required": False},
        "Day30": {"type": "float", "required": False},
        "Day31": {"type": "float", "required": False},
    }

    @staticmethod
    def parse_date_with_formats(date_string: str, formats: List[str]) -> Optional[datetime]:
        """
        Try parsing a date string using multiple formats
        
        Args:
            date_string: The date string to parse
            formats: List of format strings to try
            
        Returns:
            Parsed datetime object or None if parsing fails
        """
        if not date_string:
            logger.debug(f"Empty date string provided, returning None")
            return None

        # Ensure formats is a list
        if isinstance(formats, str):
            logger.debug(f"Converting single format '{formats}' to list")
            formats = [formats]

        # Try each format
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_string, fmt)
                logger.debug(f"Successfully parsed date '{date_string}' with format '{fmt}'")
                return parsed_date
            except ValueError:
                logger.debug(f"Failed to parse date '{date_string}' with format '{fmt}'")
                continue

        # If all formats fail, return None
        logger.warning(f"Failed to parse date '{date_string}' with any provided formats: {formats}")
        return None

    def __init__(self):
        pass

    def _handle_errors(self, errors: List[str], response: ResponseBase) -> bool:
        """
        Helper method to check for errors and update the response accordingly.
        
        Args:
            errors: List of error messages to check.
            response: Response object to update if errors exist.
            
        Returns:
            True if errors exist, False otherwise.
        """
        if len(errors) > 0:
            logger.warning(f"Handling {len(errors)} errors in response")
            for idx, err in enumerate(errors):
                logger.error(f"Error {idx+1}: {err}")
            response.errors.extend(errors)
            response.is_success = False
            return True
        logger.debug("No errors to handle in response")
        return False

    def _load_and_process_mapping_file(
        self, mapping_file: FileModel, errors: List[str]
    ) -> Optional[Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, Union[str, int, None]]]]]:
        """
        Loads and processes the mapping file to create lookup dictionaries.
        Returns a tuple of dictionaries or None if a critical error occurs.
        """
        logger.info(f"Processing mapping file: {mapping_file.name}")

        try:
            # Decode using UTF-8-SIG encoding to remove BOM if present
            tables_data_str = mapping_file.content.decode('utf-8-sig')
            logger.debug(f"Successfully decoded mapping file content ({len(tables_data_str)} characters)")
            tables_data_reader = csv.reader(io.StringIO(tables_data_str))
            logger.debug("Created CSV reader for mapping file to access columns by index")
        except Exception as decode_error:
            error_msg = f"Error decoding mapping file {mapping_file.name}: {str(decode_error)}"
            logger.error(error_msg, exc_info=True)
            errors.append(error_msg)
            return None

        division_to_subdivision: List[Dict[str, str]] = []
        divisionno_to_division: List[Dict[str, str]] = []
        division_state_days: List[Dict[str, Union[str, int, None]]] = []
        
        mapping_errors_count = 0  # For potential future use to count row-specific parsing errors

        logger.info("Building lookup dictionaries from mapping file using column indices")
        tables_data = list(tables_data_reader)

        if not tables_data:
            error_msg = f"Mapping file {mapping_file.name} is empty or contains no header row."
            logger.error(error_msg)
            errors.append(error_msg)
            return None
            
        mapping_file_headers = tables_data[0]
        data_rows = tables_data[1:] # Skip header row for data processing

        # The col 0 and 1 of the mapping file is for Division and Sub Division
        try:
            div_subdiv_header_indices: Tuple[int, int] = (0, 1)
            division_subdivision_headers: List[str] = [mapping_file_headers[i] for i in div_subdiv_header_indices]
            division_to_subdivision_raw: List[List[str]] = [[row[i] for i in div_subdiv_header_indices if i < len(row)] for row in data_rows]
            division_to_subdivision: List[Dict[str, str]] = [
                dict(zip(division_subdivision_headers, row_values))
                for row_values in division_to_subdivision_raw
                if len(row_values) == len(division_subdivision_headers) and all(row_values)
            ]
        except IndexError:
            errors.append(f"Mapping file {mapping_file.name} has insufficient columns for Division/Sub Division mapping.")
            logger.warning(f"Skipping Division/Sub Division mapping due to insufficient columns in {mapping_file.name}.")


        # The col 3 and 4 of the mapping file is for DivisionNo and Division
        try:
            divno_div_header_indices: Tuple[int, int] = (3, 4)
            divisionno_division_headers: List[str] = [mapping_file_headers[i] for i in divno_div_header_indices]
            divisionno_to_division_raw: List[List[str]] = [[row[i] for i in divno_div_header_indices if i < len(row)] for row in data_rows]
            divisionno_to_division: List[Dict[str, str]] = [
                dict(zip(divisionno_division_headers, row_values))
                for row_values in divisionno_to_division_raw
                if len(row_values) == len(divisionno_division_headers) and all(row_values)
            ]
        except IndexError:
            errors.append(f"Mapping file {mapping_file.name} has insufficient columns for DivisionNo/Division mapping.")
            logger.warning(f"Skipping DivisionNo/Division mapping due to insufficient columns in {mapping_file.name}.")

        # The col 6, 7, 9 of the mapping file is for Division Name, State, Days
        try:
            div_state_days_indices: Tuple[int, int, int] = (6, 7, 9)
            division_state_days_headers: List[str] = [mapping_file_headers[i] for i in div_state_days_indices]
            
            for row in data_rows:
                if all(i < len(row) for i in div_state_days_indices): # Ensure all indices are within row bounds
                    raw_row_list = [row[div_state_days_indices[0]], row[div_state_days_indices[1]], row[div_state_days_indices[2]]]
                    entry = dict(zip(division_state_days_headers, raw_row_list))
                    try:
                        days_value_str = str(entry.get("Days", "")).strip()
                        if days_value_str:  # Ensure not empty string
                            entry["Days"] = int(float(days_value_str))  # Convert to float first for robustness, then int
                        else:
                            entry["Days"] = None 
                    except ValueError:
                        logger.warning(f"Could not convert 'Days' value '{entry.get('Days')}' to int for entry: {entry} in {mapping_file.name}")
                        entry["Days"] = None 
                    
                    if entry.get(mapping_file_headers[div_state_days_indices[1]]) and entry.get(mapping_file_headers[div_state_days_indices[0]]): # Check using actual header names for State and Division Name
                         division_state_days.append(entry)
                else:
                    logger.warning(f"Skipping mapping row in {mapping_file.name} due to insufficient columns for Division/State/Days: {row}")
        except IndexError:
            errors.append(f"Mapping file {mapping_file.name} has insufficient columns for Division/State/Days mapping.")
            logger.warning(f"Skipping Division/State/Days mapping due to insufficient columns in {mapping_file.name}.")


        logger.info(f"Completed processing {len(tables_data)} rows from mapping file: {mapping_file.name}")
        logger.info(f"Created lookup dictionaries: division_to_subdivision ({len(division_to_subdivision)} entries), "
                   f"divisionno_to_division ({len(divisionno_to_division)} entries), "
                   f"division_state_days ({len(division_state_days)} entries)")

        if mapping_errors_count > 0: # This count is not currently incremented but is here for structure
            logger.warning(f"Encountered {mapping_errors_count} issues while processing mapping file rows from {mapping_file.name}.")

        return division_to_subdivision, divisionno_to_division, division_state_days

    async def process_uploaded_file(self, mapping_file: 'FileModel',
                                    data_files: List['FileModel']) -> 'ResponseBase':

        """
                Processes multiple daily Sales Aged Balance reports, computes values for columns 43 to 55,
                and returns a FileModel with the combined processed data.
    
                Args:
                    mapping_file: CSV file containing mapping tables
                    data_files: List of CSV files containing daily sales data with state info in filenames

                Returns:
                    ResponseBase object with success status and FileModel data
                """
        method_start_time: float = time.time()
        logger.info("=== Starting process_uploaded_file ===")
        logger.info(f"Received mapping file: {mapping_file.name} ({len(mapping_file.content)} bytes)")
        logger.info(f"Received {len(data_files)} data files")

        # Log data file names
        for idx, file in enumerate(data_files):
            logger.info(f"Data file {idx+1}: {file.name} ({len(file.content)} bytes)")

        errors: List[str] = []
        response: ResponseBase = ResponseBase(is_success=True)
        all_files_filtered_data: List[Dict[str, Any]] = []

        try:
            today: datetime = datetime.today()
            date_str: str = today.strftime("%Y%m%d")
            logger.debug(f"Processing date: {today}, formatted as {date_str}")

            import re
            logger.debug("Imported re module for regex operations")

            # Headers for template sheet
            headers: List[str] = [
                "Classification", "Sale_No", "Description", "Division", "BDM", "Sale_Date",
                "Gross_Tot", "Delot_Ind", "Cheque_Date", "Day0", "Day1", "Day2", "Day3",
                "Day4", "Day5", "Day6", "Day7", "Day8", "Day9", "Day10", "Day11",
                "Day12", "Day13", "Day14", "Day15", "Day16", "Day17", "Day18", "Day19",
                "Day20", "Day21", "Day22", "Day23", "Day24", "Day25", "Day26", "Day27",
                "Day28", "Day29", "Day30", "Day31", "State", "State-Division Name",
                "Payment Days", "Due Date", "Division Name", "Sub Division Name",
                "Gross Amount", "Collected", "To be Collected", "Payable to Vendor",
                "Month", "Year", "Cheque Date Y/N", "Days Late for Vendors Pmt", "Comments"
            ]

            # Check if data files are provided
            if not data_files or len(data_files) == 0:
                error_msg: str = "No data files provided"
                errors.append(error_msg)
                logger.error(error_msg)
                logger.info("Terminating processing due to missing data files")
                self._handle_errors(errors, response)
                return response

            # Combined processed data from all files
            all_processed_data: List[Dict[str, Any]] = []
            logger.debug("Initialized empty list for all_processed_data")

            # Process each data file
            file_count: int = len(data_files)
            logger.info(f"Beginning processing of {file_count} data files")

            for file_idx, data_file in enumerate(data_files, 1):
                file_start_time: float = time.time()
                logger.info(f"Processing file {file_idx}/{file_count}: {data_file.name}")

                # Extract state from filename using regex pattern
                # Multiple supported formats:
                # 1. "Sales Aged Balance [state].csv" (space-separated)
                # 2. "SalesAgedBalance[state].csv" (camel case)
                # 3. "Sales_Aged_Balance_[state].csv" (underscore-separated)
                state_match: Optional[re.Match] = re.search(r'(?:Sales[ _]?Aged[ _]?Balance[ _]?|SalesAgedBalance)(\w+)\.csv', data_file.name, re.IGNORECASE)
                if not state_match:
                    error_msg: str = (f"Unable to extract state from filename: {data_file.name}. Expected formats: "
                                f"'Sales Aged Balance [state].csv', 'SalesAgedBalance[state].csv', or 'Sales_Aged_Balance_[state].csv'")
                    errors.append(error_msg)
                    logger.error(error_msg)
                    logger.warning(f"Skipping file {data_file.name} due to invalid filename format")
                    continue

                state: str = state_match.group(1).upper()  # Convert to uppercase for consistency
                logger.info(f"Extracted state '{state}' from filename {data_file.name}")

                # Load data file (daily sheet) using DictReader for direct dictionary creation
                logger.debug(f"Decoding content of {data_file.name}")
                try:
                    daily_data_str: str = data_file.content.decode('utf-8')
                    logger.debug(f"Successfully decoded file content ({len(daily_data_str)} characters)")
                    daily_data_reader = csv.DictReader(io.StringIO(daily_data_str))
                    logger.debug(f"Created CSV DictReader for file {data_file.name}")
                except Exception as decode_error:
                    error_msg: str = f"Error decoding file {data_file.name}: {str(decode_error)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append(error_msg)
                    continue

                # Process rows using the import schema
                daily_data: List[Dict[str, Any]] = []
                row_count: int = 0
                conversion_errors: int = 0

                logger.info(f"Beginning row processing for {data_file.name}")
                for row_dict in daily_data_reader:
                    row_count += 1
                    if row_count % 100 == 0:
                        logger.debug(f"Processed {row_count} rows from {data_file.name}")

                    # Apply schema-based conversions
                    converted_row: Dict[str, Any] = {}
                    row_errors: int = 0

                    for field, value in row_dict.items():
                        # Skip if field is not in schema
                        if field not in self.daily_data_import_schema.keys():
                            logger.debug(f"Field '{field}' not in schema, keeping original value: {value}")
                            converted_row[field] = value
                            continue

                        field_schema = self.daily_data_import_schema[field]

                        # Skip empty values unless required
                        if not value and not field_schema.get("required", False):
                            converted_row[field] = None
                            continue
                        elif not value and field_schema.get("required", False):
                            logger.warning(f"Missing required field '{field}' in row {row_count}")
                            row_errors += 1

                        field_type: str = field_schema.get("type")
                        try:
                            # Convert based on field type
                            if field_type == "datetime" and value:
                                # Use the helper method for date parsing
                                date_formats: List[str] = field_schema.get("formats", ["%Y-%m-%d"])
                                parsed_date: Optional[datetime] = self.parse_date_with_formats(value, date_formats)
                                converted_row[field] = parsed_date if parsed_date else value
                                if parsed_date is None:
                                    logger.warning(f"Could not parse date '{value}' for field '{field}' in row {row_count}")
                                    row_errors += 1

                            elif field_type == "float" and value:
                                converted_row[field] = float(value)
                                logger.debug(f"Converted '{field}' value '{value}' to float: {converted_row[field]}")
                            elif field_type == "integer" and value:
                                converted_row[field] = int(value)
                                logger.debug(f"Converted '{field}' value '{value}' to integer: {converted_row[field]}")
                            elif field_type == "boolean":
                                # Handle various boolean string representations
                                if isinstance(value, str):
                                    converted_row[field] = value.upper() in ["TRUE", "YES", "Y", "1"]
                                else:
                                    converted_row[field] = bool(value)
                                logger.debug(f"Converted '{field}' value '{value}' to boolean: {converted_row[field]}")
                            else:
                                # Default: keep as string or original value
                                converted_row[field] = value
                        except (ValueError, TypeError) as e:
                            # If conversion fails, keep original value and continue
                            logger.warning(f"Failed to convert field '{field}' with value '{value}' to {field_type}: {str(e)}")
                            converted_row[field] = value
                            row_errors += 1

                    if row_errors > 0:
                        conversion_errors += 1
                        logger.debug(f"Row {row_count} had {row_errors} conversion errors")

                    daily_data.append(converted_row)

                logger.info(f"Completed processing {row_count} rows from {data_file.name}")
                logger.info(f"Found {conversion_errors} rows with conversion errors")

                # Clean data: Filter out rows based on conditions
                logger.info(f"Filtering data for {data_file.name}, starting with {len(daily_data)} rows")
                filtered_daily_data: List[Dict[str, Any]] = []
                excluded_count: Dict[str, int] = {
                    "cheque_date": 0,
                    "zero_gross": 0,
                    "cancellation": 0,
                    "totals_rows": 0
                }

                # -------------------------------------------------------------------------
                # DATA FILTERING SECTION
                # This section filters out rows from the daily data based on business rules.
                # Rows are excluded if they:
                #   1. Have a Cheque_Date (indicating already processed transactions)
                #   2. Have zero Gross_Tot (indicating no financial value)
                #   3. Contain "Buyer Cancellation Fees" in the description (special handling transactions)
                #   4. Are summary/total rows with specific descriptions
                # The exclusion count is tracked for reporting and audit purposes
                # -------------------------------------------------------------------------
                for row_idx, row_dict in enumerate(daily_data):
                    # Extract key fields needed for filtering decisions
                    cheque_date: Optional[datetime] = row_dict.get('Cheque_Date')
                    gross_tot: Optional[float] = row_dict.get('Gross_Tot')
                    description: Optional[str] = row_dict.get('Description')
                    classification: Optional[str] = row_dict.get('Classification')
                
                    # Skip rows that meet exclusion criteria
                    if cheque_date is not None:
                        # Exclusion Rule 1: Skip rows with a Cheque_Date
                        # Rationale: Rows with cheque dates represent transactions that are 
                        # already processed and should not be included in the aging report
                        excluded_count["cheque_date"] += 1
                        logger.debug(f"Excluding row {row_idx}: Non-null Cheque_Date={cheque_date}")
                        continue
                        
                    if gross_tot == 0:
                        # Exclusion Rule 2: Skip rows with zero gross total
                        # Rationale: Zero-value transactions don't contribute financially
                        # to the aging report and are typically informational entries
                        excluded_count["zero_gross"] += 1
                        logger.debug(f"Excluding row {row_idx}: Zero Gross_Tot")
                        continue
                        
                    if description and "Buyer Cancellation Fees" in str(description):
                        # Exclusion Rule 3: Skip cancellation fee rows
                        # Rationale: Cancellation fees have special handling requirements
                        # and are not part of the standard aging calculation
                        excluded_count["cancellation"] += 1
                        logger.debug(f"Excluding row {row_idx}: Buyer Cancellation Fees in description")
                        continue
                        
                    if description and any(total_text in str(description) for total_text in ['Total Invoices', 'Total Payments', 'Total Bankings']):
                        # Exclusion Rule 4: Skip summary/total rows 
                        # Rationale: These are calculated totals in the source data
                        # and should not be included to avoid double-counting
                        excluded_count["totals_rows"] += 1
                        logger.debug(f"Excluding row {row_idx}: Found totals text in description: '{description}'")
                        continue
                    
                    # If the row passes all exclusion criteria, it will continue
                    # to the next section below where we add the state and
                    # append it to the filtered_daily_data list

                    # Add state to the row
                    row_dict['State'] = state
                    filtered_daily_data.append(row_dict)

                logger.info(f"Filtering complete. Kept {len(filtered_daily_data)} rows, excluded {len(daily_data) - len(filtered_daily_data)} rows")
                logger.info(f"Exclusion breakdown: {excluded_count}")
                all_files_filtered_data.extend(filtered_daily_data) # MODIFICATION: Accumulate current file's filtered data

            # Load mapping file (tables sheet) using csv.reader to access by column indices
            mapping_data = self._load_and_process_mapping_file(mapping_file, errors)
            if mapping_data is None:
                # Errors list has been updated by _load_and_process_mapping_file
                self._handle_errors(errors, response)
                return response
            
            division_to_subdivision, divisionno_to_division, division_state_days = mapping_data

            # Process each row and compute new columns
            new_rows: List[Dict[str, Any]] = []
            for row_dict in all_files_filtered_data:
                new_row: Dict[str, Any] = row_dict.copy()
                if not row_dict.get('Classification'):
                    new_rows.append(new_row)
                    continue

                # Column 43 (AQ): Concatenate State and Division Name
                state_val: str = row_dict.get('State') or ""
                division_name: str = ""  # Will be set in AT
                new_row['State-Division Name'] = f"{state_val}-{division_name}" if state_val else ""

                # Column 46 (AT): Lookup Division from DivisionNo
                division_no: Union[str, int, None] = row_dict.get('Division')
                # Find matching division entry
                division_entry: Optional[Dict[str, str]] = next((item for item in divisionno_to_division if item.get("DivisionNo") == division_no), None)
                new_row['Division Name'] = division_entry.get("Division", "") if division_entry else ""
                # Update AQ with Division Name
                new_row['State-Division Name'] = f"{state_val}-{new_row['Division Name']}" if state_val and new_row[
                    'Division Name'] else ""

                # Column 44 (AR): Lookup Payment Days
                state_division: str = new_row['State-Division Name']
                # Find matching entry for state-division
                state_days_entry: Optional[Dict[str, Any]] = next((item for item in division_state_days 
                                        if f"{item.get('State')}-{item.get('Division Name')}" == state_division), None)
                new_row['Payment Days'] = state_days_entry.get("Days", "") if state_days_entry else ""

                # Column 45 (AS): Add Sale_Date and Payment Days
                sale_date: Optional[datetime] = row_dict.get('Sale_Date')
                payment_days: Union[int, str, None] = new_row['Payment Days']
                if isinstance(sale_date, datetime) and isinstance(payment_days, int):
                    new_row['Due Date'] = sale_date + timedelta(days=payment_days)
                else:
                    new_row['Due Date'] = ""
                    if not isinstance(payment_days, int):
                        logger.debug(f"Payment days '{payment_days}' is not an int for Sale_No {row_dict.get('Sale_No')}, Sale_Date '{sale_date}'. Due Date set to empty.")

                # Column 47 (AU): Lookup Sub Division
                division: Optional[str] = new_row['Division Name']
                # Find matching subdivision entry
                subdivision_entry: Optional[Dict[str, str]] = next((item for item in division_to_subdivision 
                                         if item.get("Division") == division), None)
                new_row['Sub Division Name'] = subdivision_entry.get("Sub Division", "") if subdivision_entry else ""

                # Column 48 (AV): Compute Gross Amount
                delot_ind: bool = str(row_dict.get('Delot_Ind', "")).upper() == "TRUE"
                gross_tot: Union[float, int, None] = row_dict.get('Gross_Tot')
                sale_no: Union[float, int, str, None] = row_dict.get('Sale_No')
                current_gross_amount_num: float = 0.0
                if delot_ind and isinstance(gross_tot, (int, float)) and isinstance(sale_no, (int, float)):
                    current_gross_amount_num = float(gross_tot - sale_no)
                elif isinstance(gross_tot, (int, float)):
                    current_gross_amount_num = float(gross_tot)
                new_row['Gross Amount'] = current_gross_amount_num

                # Column 50 (AX): Get Day value for today - this becomes 'To be Collected'
                day_num: int = today.day
                day_key: str = f"Day{day_num}"
                numeric_to_be_collected: float = 0.0
                source_tbc_val: Union[float, int, None] = row_dict.get(day_key, None)
                if isinstance(source_tbc_val, (int, float)):
                    numeric_to_be_collected = float(source_tbc_val)
                new_row['To be Collected'] = numeric_to_be_collected

                # Column 49 (AW): Calculate 'Collected' = Gross Amount - To be Collected
                # This definition is based on the desired output reconciliation.
                collected_calc: float = 0.0
                if isinstance(current_gross_amount_num, (int, float)) and isinstance(numeric_to_be_collected, (int, float)):
                    collected_calc = current_gross_amount_num - numeric_to_be_collected
                new_row['Collected'] = collected_calc
                
                # Column 51 (AY): Compute Payable to Vendor
                payable_to_vendor_val_num: float = 0.0 
                # Logic: if Delot_Ind is TRUE and To Be Collected is 0, then Payable to Vendor = Gross Amount. Otherwise 0.
                if delot_ind:
                    if numeric_to_be_collected == 0.0: 
                        payable_to_vendor_val_num = current_gross_amount_num
                new_row['Payable to Vendor'] = payable_to_vendor_val_num
                
                # Column 52 (AZ): Format Sale_Date as MMM-YY
                if row_dict.get('Description'):
                    if isinstance(sale_date, datetime):
                        new_row['Month'] = sale_date.strftime("%b-%y")
                    else:
                        new_row['Month'] = ""
                else:
                    new_row['Month'] = ""

                # Column 53 (BA): Extract year from Sale_Date
                if row_dict.get('Description'):
                    if isinstance(sale_date, datetime):
                        new_row['Year'] = sale_date.year
                    else:
                        new_row['Year'] = ""
                else:
                    new_row['Year'] = ""

                # Column 54 (BB): Check To be Collected (Interpreted as Cheque Date Y/N based on output)
                if row_dict.get('Cheque_Date'): # If Cheque_Date has any value
                    new_row['Cheque Date Y/N'] = "YES"
                else:
                    new_row['Cheque Date Y/N'] = "NO"

                # Column 55 (BC): Compute days late
                try:
                    if new_row['Payable to Vendor'] == row_dict.get('Gross_Tot'):
                        cheque_date = row_dict.get('Cheque_Date')
                        if isinstance(cheque_date, datetime):
                            days_diff: int = (today - cheque_date.date()).days
                            new_row['Days Late for Vendors Pmt'] = days_diff if days_diff > 0 else ""
                        else:
                            new_row['Days Late for Vendors Pmt'] = ""
                    else:
                        new_row['Days Late for Vendors Pmt'] = ""
                except:
                    new_row['Days Late for Vendors Pmt'] = ""

                new_rows.append(new_row)

            # Add processed rows to the combined result
            all_processed_data.extend(new_rows)

            # Create the output workbook here (end of method)
            template_wb = openpyxl.Workbook()
            template_sheet = template_wb.active
            template_sheet.title = "---DATA---"

            # Create a Tables sheet
            tables_sheet = template_wb.create_sheet(title="Tables")

            # Populate the Tables sheet with original mapping data
            # Convert mapping file back to rows for the Tables sheet
            # We'll preserve the original structure since we're only changing how we read it
            tables_data_str: str = mapping_file.content.decode('utf-8')
            tables_data_csv = csv.reader(io.StringIO(tables_data_str))
            tables_data_rows: List[List[str]] = list(tables_data_csv)

            logger.debug(f"Copying {len(tables_data_rows)} rows to Tables sheet in output workbook")
            for row_idx, row in enumerate(tables_data_rows, 1):
                for col_idx, value in enumerate(row, 1):
                    tables_sheet.cell(row=row_idx, column=col_idx).value = value

            # Add headers to template sheet
            for i, header in enumerate(headers, 1):
                template_sheet.cell(row=1, column=i).value = header

            # Define column indices for formatting (1-based)
            # Ensure these header names exactly match those in your `headers` list
            col_gross_amount_idx: int = headers.index("Gross Amount") + 1
            col_collected_idx: int = headers.index("Collected") + 1
            col_tbc_idx: int = headers.index("To be Collected") + 1
            col_ptv_idx: int = headers.index("Payable to Vendor") + 1
            col_due_date_idx: int = headers.index("Due Date") + 1
            
            custom_number_format: str = "_(* #,##0.00_);_(* (#,##0.00);_(* \"-\"??_);_(@_)"
            date_format_dd_mmm_yy: str = "DD-MMM-YY"

            # Append all processed rows to template
            current_row_excel: int = 2 # Start from row 2 for data
            for row_dict in all_processed_data:
                row_values: List[Any] = []
                for header_key in headers: 
                    val = row_dict.get(header_key, None)
                    # Ensure datetime objects are naive if they are timezone-aware, or handle as needed
                    # openpyxl works best with naive datetime objects or timezone-aware ones if Excel is set up for it.
                    if isinstance(val, datetime) and val.tzinfo is not None:
                        val = val.replace(tzinfo=None) # Example: make naive
                    row_values.append(val)
                template_sheet.append(row_values)

                # Apply formatting for specific columns in the current row
                template_sheet.cell(row=current_row_excel, column=col_gross_amount_idx).number_format = custom_number_format
                template_sheet.cell(row=current_row_excel, column=col_collected_idx).number_format = custom_number_format
                template_sheet.cell(row=current_row_excel, column=col_tbc_idx).number_format = custom_number_format
                template_sheet.cell(row=current_row_excel, column=col_ptv_idx).number_format = custom_number_format
                
                # Apply date format for Due Date
                # Check if the cell actually contains a date (it might be "" if Due Date couldn't be calculated)
                due_date_val = template_sheet.cell(row=current_row_excel, column=col_due_date_idx).value
                if isinstance(due_date_val, datetime):
                    template_sheet.cell(row=current_row_excel, column=col_due_date_idx).number_format = date_format_dd_mmm_yy
                
                current_row_excel +=1

            # Save the workbook to bytes
            output: io.BytesIO = io.BytesIO()
            template_wb.save(output)
            output.seek(0)

            # Create a descriptive file name
            file_name: str = f"Sales_Aged_Balance_Report_{date_str}_pygrays_api.xlsx"

            # Set the data in the response object
            response.data = FileModel(name=file_name, content=output.getvalue())
            return response

        except Exception as e:
            # Log the error and return error response
            end_time: float = time.time() - method_start_time
            error_message: str = f"Error processing file: {str(e)}"
            logger.error(error_message, exc_info=True)
            errors.append(error_message)
            self._handle_errors(errors, response)
            return response
