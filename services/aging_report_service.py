import io
import csv
import time
from datetime import datetime, timedelta

import openpyxl
from fastapi import UploadFile, HTTPException

from models.file_model import FileModel
from models.response_base import ResponseBase
from services.multi_logging import LoggingService


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
            return None
            
        # Ensure formats is a list
        if isinstance(formats, str):
            formats = [formats]
            
        # Try each format
        for fmt in formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
                
        # If all formats fail, return None
        return None

    def __init__(self):
        pass

    async def process_uploaded_file(self, state: str, mapping_file: 'FileModel',
                                    data_file: 'FileModel') -> 'ResponseBase':

        """
                Processes a daily Sales Aged Balance report, computes values for columns 43 to 55,
                and returns a FileModel with the processed data.

                Args:
                    state: The state code to use for processing
                    mapping_file: CSV file containing mapping tables
                    data_file: CSV file containing daily sales data

                Returns:
                    ResponseBase object with success status and FileModel data
                """
        try:
            start_time = time.time()
            today = datetime.today()
            date_str = today.strftime("%Y%m%d")

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

            # Check if state is provided
            if not state:
                return ResponseBase(is_success=False, errors=["State is required"])

            # Load data file (daily sheet) using DictReader for direct dictionary creation
            daily_data_str = data_file.content.decode('utf-8')
            daily_data_reader = csv.DictReader(io.StringIO(daily_data_str))
            
            # Process rows using the import schema
            daily_data = []
            for row_dict in daily_data_reader:
                # Apply schema-based conversions
                converted_row = {}
                for field, value in row_dict.items():
                    # Skip if field is not in schema
                    if field not in self.daily_data_import_schema.keys():
                        converted_row[field] = value
                        continue
                    
                    field_schema = self.daily_data_import_schema[field]
                    
                    # Skip empty values unless required
                    if not value and not field_schema.get("required", False):
                        converted_row[field] = None
                        continue
                    
                    field_type = field_schema.get("type")
                    try:
                        # Convert based on field type
                        if field_type == "datetime" and value:
                            # Use the helper method for date parsing
                            date_formats = field_schema.get("formats", ["%Y-%m-%d"])
                            parsed_date = self.parse_date_with_formats(value, date_formats)
                            converted_row[field] = parsed_date if parsed_date else value
                                
                        elif field_type == "float" and value:
                            converted_row[field] = float(value)
                        elif field_type == "integer" and value:
                            converted_row[field] = int(value)
                        elif field_type == "boolean":
                            # Handle various boolean string representations
                            if isinstance(value, str):
                                converted_row[field] = value.upper() in ["TRUE", "YES", "Y", "1"]
                            else:
                                converted_row[field] = bool(value)
                        else:
                            # Default: keep as string or original value
                            converted_row[field] = value
                    except (ValueError, TypeError) as e:
                        # If conversion fails, keep original value and continue
                        converted_row[field] = value
                
                daily_data.append(converted_row)
            
            # Clean data: Filter out rows based on conditions
            filtered_daily_data = []
            for row_dict in daily_data:
                cheque_date = row_dict.get('Cheque_Date')
                gross_tot = row_dict.get('Gross_Tot')
                description = row_dict.get('Description')
                classification = row_dict.get('Classification')
                
                # Skip rows that meet exclusion criteria
                if (cheque_date is not None or 
                    gross_tot == 0 or 
                    (description and "Buyer Cancellation Fees" in str(description)) or
                    classification in ['Total Invoices', 'Total Payments', 'Total Bankings']):
                    continue
                
                # Add state to the row
                row_dict['State'] = state
                filtered_daily_data.append(row_dict)
            
            # Load mapping file (tables sheet) using DictReader
            tables_data_str = mapping_file.content.decode('utf-8')
            tables_data_reader = csv.DictReader(io.StringIO(tables_data_str))
            
            # Create lookup dictionaries from Tables
            division_to_subdivision = {}
            divisionno_to_division = {}
            state_division_to_days = {}
            
            for row in tables_data_reader:
                # Access fields by column names instead of indices
                division = row.get('Division', '')
                sub_division = row.get('Sub Division', '')
                division_no = row.get('Division No', '')
                division_type = row.get('Division Type', '')
                state_val = row.get('State', '')
                state_division_name = row.get('State-Division Name', '')
                
                # Convert days to integer if possible
                days = row.get('Days', '')
                try:
                    days = int(days) if days else ""
                except (ValueError, TypeError):
                    pass
                
                if division and sub_division:
                    division_to_subdivision[division] = sub_division
                if division_no and division_type:
                    divisionno_to_division[division_no] = division_type
                if state_division_name and days:
                    state_division_to_days[state_division_name] = days
            
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

            # Create the output workbook here (end of method)
            template_wb = openpyxl.Workbook()
            template_sheet = template_wb.active
            template_sheet.title = "---DATA---"
            
            # Create a Tables sheet
            tables_sheet = template_wb.create_sheet(title="Tables")
            
            # Populate the Tables sheet with original mapping data
            for row_idx, row in enumerate(tables_data_rows, 1):
                for col_idx, value in enumerate(row, 1):
                    tables_sheet.cell(row=row_idx, column=col_idx).value = value
            
            # Add headers to template sheet
            for i, header in enumerate(headers, 1):
                template_sheet.cell(row=1, column=i).value = header

            # Append new rows to template
            for row_dict in new_rows:
                row_values = [row_dict.get(header, None) for header in headers]
                template_sheet.append(row_values)

            # Save the workbook to bytes
            output = io.BytesIO()
            template_wb.save(output)
            output.seek(0)

            # Create a descriptive file name
            file_name = f"Sales_Aged_Balance_Report_{date_str}.xlsx"

            # Return the FileModel wrapped in a ResponseBase
            return ResponseBase(
                is_success=True,
                data=FileModel(name=file_name, content=output.getvalue())
            )

        except Exception as e:
            # Log the error and return error response
            end_time = time.time() - start_time
            error_message = f"Error processing file: {str(e)}"
            return ResponseBase(is_success=False, errors=[error_message])
