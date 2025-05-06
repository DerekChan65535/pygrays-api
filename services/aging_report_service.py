import os
import time
from datetime import datetime, timedelta

import openpyxl
from fastapi import UploadFile, HTTPException

from models.file_model import FileModel
from models.response_base import ResponseBase
from services.multi_logging import LoggingService


class AgingReportService:
    def __init__(self):
        pass

    async def process_uploaded_file(self, state: str ,mapping_file: FileModel, data_file: FileModel)-> ResponseBase:






        # This is a placeholder that throws a not implemented exception as requested
        return ResponseBase(is_success=False, errors=["Not implemented"])

    def formatSAR(self):
        """
        Processes a daily Sales Aged Balance report, computes values for columns 43 to 55,
        and updates the template workbook with computed values using lookup dictionaries.
        """
        start_time = time.time()
        today = datetime.today()
        date_str = today.strftime("%Y%m%d")
        daily_report_name = f"Sales Aged Balance - {date_str}.xls"
        daily_path = f"C:\\Reports\\{daily_report_name}"  # Adjust path as needed


        # Hardcoded header for ---DATA--- sheet
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

        # Load daily report
        daily_wb = openpyxl.load_workbook(daily_path)
        daily_sheet = daily_wb.active

        # Clean data: Remove rows based on conditions
        rows_to_delete = []
        for i in range(2, daily_sheet.max_row + 1):
            cheque_date = daily_sheet.cell(row=i, column=9).value  # Column I
            gross_tot = daily_sheet.cell(row=i, column=7).value  # Column G
            description = daily_sheet.cell(row=i, column=3).value  # Column C
            classification = daily_sheet.cell(row=i, column=1).value  # Column A
            if cheque_date is not None or gross_tot == 0 or "Buyer Cancellation Fees" in str(description):
                rows_to_delete.append(i)
            if classification in ['Total Invoices', 'Total Payments', 'Total Bankings']:
                rows_to_delete.append(i)

        for row_idx in sorted(rows_to_delete, reverse=True):
            daily_sheet.delete_rows(row_idx)

        # Prompt for state and add to column AP (index 41, column 42)
        state = input("Enter state: ")
        if not state:
            print("No state entered, exiting")
            daily_wb.close()
            return

        max_row = daily_sheet.max_row
        for i in range(2, max_row + 1):
            daily_sheet.cell(row=i, column=42).value = state

        # Load template workbook
        template_path = 'All Sales Aged Balance - (TEMPLATE).xlsm'
        template_wb = openpyxl.load_workbook(template_path)
        template_sheet = template_wb['---DATA---']
        tables_sheet = template_wb['Tables']

        # Create lookup dictionaries from Tables
        division_to_subdivision = {}
        divisionno_to_division = {}
        state_division_to_days = {}
        for row in tables_sheet.iter_rows(min_row=2):
            division = row[0].value
            sub_division = row[1].value
            division_no = row[3].value
            division_type = row[4].value
            division_name = row[6].value
            state_val = row[7].value
            state_division_name = row[8].value
            days = row[9].value
            if division and sub_division:
                division_to_subdivision[division] = sub_division
            if division_no and division_type:
                divisionno_to_division[division_no] = division_type
            if state_division_name and days:
                state_division_to_days[state_division_name] = days

        # Convert daily data to list of dicts
        daily_data = []
        for row in daily_sheet.iter_rows(min_row=2, max_col=42):
            row_dict = {headers[i]: cell.value for i, cell in enumerate(row)}
            daily_data.append(row_dict)

        # Process each row and compute new columns
        new_rows = []
        for row_dict in daily_data:
            new_row = row_dict.copy()
            if not row_dict['Classification']:
                new_rows.append(new_row)
                continue

            # Column 43 (AQ): Concatenate State and Division Name
            state_val = row_dict['State'] or ""
            division_name = ""  # Will be set in AT
            new_row['State-Division Name'] = f"{state_val}-{division_name}" if state_val else ""

            # Column 46 (AT): Lookup Division from DivisionNo
            division_no = row_dict['Division']
            new_row['Division Name'] = divisionno_to_division.get(division_no, "")
            # Update AQ with Division Name
            new_row['State-Division Name'] = f"{state_val}-{new_row['Division Name']}" if state_val and new_row[
                'Division Name'] else ""

            # Column 44 (AR): Lookup Payment Days
            state_division = new_row['State-Division Name']
            new_row['Payment Days'] = state_division_to_days.get(state_division, "")

            # Column 45 (AS): Add Sale_Date and Payment Days
            sale_date = row_dict['Sale_Date']
            payment_days = new_row['Payment Days']
            if isinstance(sale_date, datetime) and isinstance(payment_days, (int, float)):
                new_row['Due Date'] = sale_date + timedelta(days=payment_days)
            else:
                new_row['Due Date'] = ""

            # Column 47 (AU): Lookup Sub Division
            division = new_row['Division Name']
            new_row['Sub Division Name'] = division_to_subdivision.get(division, "")

            # Column 48 (AV): Compute Gross Amount
            delot_ind = str(row_dict['Delot_Ind']).upper() == "TRUE"
            gross_tot = row_dict['Gross_Tot']
            sale_no = row_dict['Sale_No']
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
                classification = row_dict['Classification']
                day_value = new_row['To be Collected']
                if delot_ind and isinstance(classification, (int, float)) and isinstance(day_value, (int, float)):
                    new_row['Payable to Vendor'] = classification - day_value
                else:
                    new_row['Payable to Vendor'] = 0
            else:
                new_row['Payable to Vendor'] = ""

            # Column 52 (AZ): Format Sale_Date as MMM-YY
            if row_dict['Description']:
                if isinstance(sale_date, datetime):
                    new_row['Month'] = sale_date.strftime("%b-%y").upper()
                else:
                    new_row['Month'] = ""
            else:
                new_row['Month'] = ""

            # Column 53 (BA): Extract year from Sale_Date
            if row_dict['Description']:
                if isinstance(sale_date, datetime):
                    new_row['Year'] = sale_date.year
                else:
                    new_row['Year'] = ""
            else:
                new_row['Year'] = ""

            # Column 54 (BB): Check To be Collected
            if row_dict['Cheque_Date']:
                to_be_collected = new_row['To be Collected']
                new_row['Cheque Date Y/N'] = "YES" if isinstance(to_be_collected,
                                                                 (int, float)) and to_be_collected != 0 else "NO"
            else:
                new_row['Cheque Date Y/N'] = ""

            # Column 55 (BC): Compute days late
            try:
                if new_row['Payable to Vendor'] == row_dict['Gross_Tot']:
                    cheque_date = row_dict['Cheque_Date']
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

        # Append new rows to template
        for row_dict in new_rows:
            row_values = [row_dict.get(header, None) for header in headers]
            template_sheet.append(row_values)

        # Save template workbook
        template_wb.save(template_path)

        # Close daily workbook
        daily_wb.close()

        # Show elapsed time
        end_time = time.time() - start_time
        print(f"Elapsed time: {end_time:.2f} seconds")

    # Example usage:
    # formatSAR()