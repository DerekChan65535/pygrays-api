import io
import csv
import time
import logging
import sys
import traceback
from datetime import datetime, timedelta
from typing import List, Optional

import openpyxl
from fastapi import UploadFile, HTTPException

from models.file_model import FileModel
from models.response_base import ResponseBase
from services.multi_logging import LoggingService, LogConfig


# Initialize logger with detailed configuration
log_config = LogConfig(
    level=logging.DEBUG,
    fmt="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
    log_to_console=True
)
logger = LoggingService(log_config).get_logger(__name__)


class AgingReportService:
    # Define the schema for daily data import with types and formats
    daily_data_import_schema = {
        "Classification": {"type": "string", "required": True},
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
    def parse_date_with_formats(date_string: str, formats: list) -> datetime:
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
        method_start_time = time.time()
        logger.info("=== Starting process_uploaded_file ===")
        logger.info(f"Received mapping file: {mapping_file.name} ({len(mapping_file.content)} bytes)")
        logger.info(f"Received {len(data_files)} data files")
        
        # Log data file names
        for idx, file in enumerate(data_files):
            logger.info(f"Data file {idx+1}: {file.name} ({len(file.content)} bytes)")
            
        errors = []
        response = ResponseBase(is_success=True)
        
        try:
            today = datetime.today()
            date_str = today.strftime("%Y%m%d")
            logger.debug(f"Processing date: {today}, formatted as {date_str}")
    
            import re
            logger.debug("Imported re module for regex operations")
            
            # Headers for template sheet
            headers = [
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
                error_msg = "No data files provided"
                errors.append(error_msg)
                logger.error(error_msg)
                logger.info("Terminating processing due to missing data files")
                self._handle_errors(errors, response)
                return response
                    
            # Combined processed data from all files
            all_processed_data = []
            logger.debug("Initialized empty list for all_processed_data")
                    
            # Process each data file
            file_count = len(data_files)
            logger.info(f"Beginning processing of {file_count} data files")
            
            for file_idx, data_file in enumerate(data_files, 1):
                file_start_time = time.time()
                logger.info(f"Processing file {file_idx}/{file_count}: {data_file.name}")
                
                # Extract state from filename using regex pattern
                # Multiple supported formats:
                # 1. "Sales Aged Balance [state].csv" (space-separated)
                # 2. "SalesAgedBalance[state].csv" (camel case)
                # 3. "Sales_Aged_Balance_[state].csv" (underscore-separated)
                state_match = re.search(r'(?:Sales[ _]?Aged[ _]?Balance[ _]?|SalesAgedBalance)(\w+)\.csv', data_file.name, re.IGNORECASE)
                if not state_match:
                    error_msg = (f"Unable to extract state from filename: {data_file.name}. Expected formats: "
                                f"'Sales Aged Balance [state].csv', 'SalesAgedBalance[state].csv', or 'Sales_Aged_Balance_[state].csv'")
                    errors.append(error_msg)
                    logger.error(error_msg)
                    logger.warning(f"Skipping file {data_file.name} due to invalid filename format")
                    continue
                    
                state = state_match.group(1).lower()  # Convert to lowercase for consistency
                logger.info(f"Extracted state '{state}' from filename {data_file.name}")
        
                # Load data file (daily sheet) using DictReader for direct dictionary creation
                logger.debug(f"Decoding content of {data_file.name}")
                try:
                    daily_data_str = data_file.content.decode('utf-8')
                    logger.debug(f"Successfully decoded file content ({len(daily_data_str)} characters)")
                    daily_data_reader = csv.DictReader(io.StringIO(daily_data_str))
                    logger.debug(f"Created CSV DictReader for file {data_file.name}")
                except Exception as decode_error:
                    error_msg = f"Error decoding file {data_file.name}: {str(decode_error)}"
                    logger.error(error_msg, exc_info=True)
                    errors.append(error_msg)
                    continue
                
                # Process rows using the import schema
                daily_data = []
                row_count = 0
                conversion_errors = 0
                
                logger.info(f"Beginning row processing for {data_file.name}")
                for row_dict in daily_data_reader:
                    row_count += 1
                    if row_count % 100 == 0:
                        logger.debug(f"Processed {row_count} rows from {data_file.name}")
                    
                    # Apply schema-based conversions
                    converted_row = {}
                    row_errors = 0
                    
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
                        
                        field_type = field_schema.get("type")
                        try:
                            # Convert based on field type
                            if field_type == "datetime" and value:
                                # Use the helper method for date parsing
                                date_formats = field_schema.get("formats", ["%Y-%m-%d"])
                                parsed_date = self.parse_date_with_formats(value, date_formats)
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
                filtered_daily_data = []
                excluded_count = {
                    "cheque_date": 0,
                    "zero_gross": 0,
                    "cancellation": 0,
                    "totals": 0
                }
                
                for row_idx, row_dict in enumerate(daily_data):
                    cheque_date = row_dict.get('Cheque_Date')
                    gross_tot = row_dict.get('Gross_Tot')
                    description = row_dict.get('Description')
                    classification = row_dict.get('Classification')
                    
                    # Log detailed info for every 100th row as an example
                    if row_idx % 100 == 0:
                        logger.debug(f"Filtering row {row_idx}: Cheque_Date={cheque_date}, "
                                    f"Gross_Tot={gross_tot}, Classification={classification}")
                    
                    # Skip rows that meet exclusion criteria
                    if cheque_date is not None:
                        excluded_count["cheque_date"] += 1
                        logger.debug(f"Excluding row {row_idx}: Non-null Cheque_Date={cheque_date}")
                        continue
                    if gross_tot == 0:
                        excluded_count["zero_gross"] += 1
                        logger.debug(f"Excluding row {row_idx}: Zero Gross_Tot")
                        continue
                    if description and "Buyer Cancellation Fees" in str(description):
                        excluded_count["cancellation"] += 1
                        logger.debug(f"Excluding row {row_idx}: Buyer Cancellation Fees in description")
                        continue
                    if classification in ['Total Invoices', 'Total Payments', 'Total Bankings']:
                        excluded_count["totals"] += 1
                        logger.debug(f"Excluding row {row_idx}: Classification is '{classification}'")
                        continue
                    
                    # Add state to the row
                    row_dict['State'] = state
                    filtered_daily_data.append(row_dict)
                
                logger.info(f"Filtering complete. Kept {len(filtered_daily_data)} rows, excluded {len(daily_data) - len(filtered_daily_data)} rows")
                logger.info(f"Exclusion breakdown: {excluded_count}")
            
            # Load mapping file (tables sheet) using DictReader
            logger.info(f"Processing mapping file: {mapping_file.name}")
            
            try:
                tables_data_str = mapping_file.content.decode('utf-8')
                logger.debug(f"Successfully decoded mapping file content ({len(tables_data_str)} characters)")
                tables_data_reader = csv.DictReader(io.StringIO(tables_data_str))
                logger.debug(f"Created CSV DictReader for mapping file")
            except Exception as decode_error:
                error_msg = f"Error decoding mapping file {mapping_file.name}: {str(decode_error)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)
                self._handle_errors(errors, response)
                return response
            
            # Create lookup dictionaries from Tables
            division_to_subdivision = {}
            divisionno_to_division = {}
            state_division_to_days = {}
            
            mapping_row_count = 0
            mapping_errors = 0
            
            logger.info("Building lookup dictionaries from mapping file")
            for row in tables_data_reader:
                mapping_row_count += 1
                
                # Access fields by column names instead of indices
                division = row.get('Division', '')
                sub_division = row.get('Sub Division', '')
                division_no = row.get('Division No', '')
                division_type = row.get('Division Type', '')
                state_val = row.get('State', '')
                state_division_name = row.get('State-Division Name', '')
                
                # Log every 10th row as a sample
                if mapping_row_count % 10 == 0:
                    logger.debug(f"Mapping row {mapping_row_count}: Division={division}, "
                               f"Sub Division={sub_division}, Division No={division_no}")
                
                # Convert days to integer if possible
                days = row.get('Days', '')
                try:
                    days = int(days) if days else ""
                    if days:
                        logger.debug(f"Converted 'Days' value '{row.get('Days', '')}' to integer: {days}")
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to convert Days value '{days}' to integer in mapping file row {mapping_row_count}: {str(e)}")
                    mapping_errors += 1
                    pass
                
                if division and sub_division:
                    division_to_subdivision[division] = sub_division
                    logger.debug(f"Added mapping: Division '{division}' → Sub Division '{sub_division}'")
                
                if division_no and division_type:
                    divisionno_to_division[division_no] = division_type
                    logger.debug(f"Added mapping: Division No '{division_no}' → Division Type '{division_type}'")
                
                if state_division_name and days:
                    state_division_to_days[state_division_name] = days
                    logger.debug(f"Added mapping: State-Division '{state_division_name}' → Days '{days}'")
            
            logger.info(f"Completed processing {mapping_row_count} rows from mapping file")
            logger.info(f"Created lookup dictionaries: division_to_subdivision ({len(division_to_subdivision)} entries), "
                       f"divisionno_to_division ({len(divisionno_to_division)} entries), "
                       f"state_division_to_days ({len(state_division_to_days)} entries)")
            
            if mapping_errors > 0:
                logger.warning(f"Encountered {mapping_errors} errors while processing mapping file")
            
            # Process each row and compute new columns
            new_rows = []
            for row_dict in filtered_daily_data:
                new_row = row_dict.copy()
                if not row_dict.get('Classification'):
                    new_rows.append(new_row)
                    continue

                # Column 43 (AQ): Concatenate State and Division Name
                state_val = row_dict.get('State') or ""
                division_name = ""  # Will be set in AT
                new_row['State-Division Name'] = f"{state_val}-{division_name}" if state_val else ""

                # Column 46 (AT): Lookup Division from DivisionNo
                division_no = row_dict.get('Division')
                new_row['Division Name'] = divisionno_to_division.get(division_no, "")
                # Update AQ with Division Name
                new_row['State-Division Name'] = f"{state_val}-{new_row['Division Name']}" if state_val and new_row[
                    'Division Name'] else ""

                # Column 44 (AR): Lookup Payment Days
                state_division = new_row['State-Division Name']
                new_row['Payment Days'] = state_division_to_days.get(state_division, "")

                # Column 45 (AS): Add Sale_Date and Payment Days
                sale_date = row_dict.get('Sale_Date')
                payment_days = new_row['Payment Days']
                if isinstance(sale_date, datetime) and isinstance(payment_days, (int, float)):
                    new_row['Due Date'] = sale_date + timedelta(days=payment_days)
                else:
                    new_row['Due Date'] = ""

                # Column 47 (AU): Lookup Sub Division
                division = new_row['Division Name']
                new_row['Sub Division Name'] = division_to_subdivision.get(division, "")

                # Column 48 (AV): Compute Gross Amount
                delot_ind = str(row_dict.get('Delot_Ind', "")).upper() == "TRUE"
                gross_tot = row_dict.get('Gross_Tot')
                sale_no = row_dict.get('Sale_No')
                if delot_ind and isinstance(gross_tot, (int, float)) and isinstance(sale_no, (int, float)):
                    new_row['Gross Amount'] = gross_tot - sale_no
                else:
                    new_row['Gross Amount'] = gross_tot if isinstance(gross_tot, (int, float)) else ""

                # Column 49 (AW): Subtract Collected from Gross Amount
                gross_amount = new_row['Gross Amount']
                collected = new_row.get('Collected', 0)  # Default to 0 if not set
                try:
                    if isinstance(gross_amount, (int, float)) and isinstance(collected, (int, float)):
                        new_row['Collected'] = gross_amount - collected
                    else:
                        new_row['Collected'] = ""
                except:
                    new_row['Collected'] = ""

                # Column 50 (AX): Get Day value for today
                day_num = today.day
                day_key = f"Day{day_num}"
                new_row['To be Collected'] = row_dict.get(day_key, None)

                # Column 51 (AY): Compute difference based on Delot_Ind
                if new_row['To be Collected'] is not None:
                    classification = row_dict.get('Classification')
                    day_value = new_row['To be Collected']
                    if delot_ind and isinstance(classification, (int, float)) and isinstance(day_value, (int, float)):
                        new_row['Payable to Vendor'] = classification - day_value
                    else:
                        new_row['Payable to Vendor'] = 0
                else:
                    new_row['Payable to Vendor'] = ""

                # Column 52 (AZ): Format Sale_Date as MMM-YY
                if row_dict.get('Description'):
                    if isinstance(sale_date, datetime):
                        new_row['Month'] = sale_date.strftime("%b-%y").upper()
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

                # Column 54 (BB): Check To be Collected
                if row_dict.get('Cheque_Date'):
                    to_be_collected = new_row['To be Collected']
                    new_row['Cheque Date Y/N'] = "YES" if isinstance(to_be_collected,
                                                                     (int, float)) and to_be_collected != 0 else "NO"
                else:
                    new_row['Cheque Date Y/N'] = ""

                # Column 55 (BC): Compute days late
                try:
                    if new_row['Payable to Vendor'] == row_dict.get('Gross_Tot'):
                        cheque_date = row_dict.get('Cheque_Date')
                        if isinstance(cheque_date, datetime):
                            days_diff = (today - cheque_date.date()).days
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
            tables_data_str = mapping_file.content.decode('utf-8')
            tables_data_csv = csv.reader(io.StringIO(tables_data_str))
            tables_data_rows = list(tables_data_csv)
            
            for row_idx, row in enumerate(tables_data_rows, 1):
                for col_idx, value in enumerate(row, 1):
                    tables_sheet.cell(row=row_idx, column=col_idx).value = value
            
            # Add headers to template sheet
            for i, header in enumerate(headers, 1):
                template_sheet.cell(row=1, column=i).value = header
            
            # Append all processed rows to template
            for row_dict in all_processed_data:
                row_values = [row_dict.get(header, None) for header in headers]
                template_sheet.append(row_values)

            # Save the workbook to bytes
            output = io.BytesIO()
            template_wb.save(output)
            output.seek(0)

            # Create a descriptive file name
            file_name = f"Sales_Aged_Balance_Report_{date_str}.xlsx"

            # Set the data in the response object
            response.data = FileModel(name=file_name, content=output.getvalue())
            return response

        except Exception as e:
            # Log the error and return error response
            end_time = time.time() - start_time
            error_message = f"Error processing file: {str(e)}"
            logger.error(error_message, exc_info=True)
            errors.append(error_message)
            self._handle_errors(errors, response)
            return response
