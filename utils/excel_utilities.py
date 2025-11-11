import io
import csv
import logging
from datetime import datetime
from typing import Optional, List

from openpyxl import load_workbook, Workbook
import xlwt
import xlrd

from models.file_model import FileModel

# Initialize logger
logger = logging.getLogger(__name__)


class ExcelUtilities:
    """
    Utility class for Excel operations
    Supports both XLS and XLSX file formats.
    """
    
    @staticmethod
    def convert_xlsx_to_xls(xlsx_content: bytes, filename: str, errors: Optional[List[str]] = None) -> Optional[bytes]:
        """
        Converts XLSX file content (bytes) to XLS format.
        
        Args:
            xlsx_content: Bytes content of the XLSX file
            filename: Original filename (for logging)
            errors: Optional list to append conversion errors
            
        Returns:
            Bytes content of XLS file if conversion successful, None otherwise
        """
        if errors is None:
            errors = []
        logger.info(f"Converting XLSX to XLS format: {filename}")
        try:
            wb = load_workbook(io.BytesIO(xlsx_content), read_only=True, data_only=True)
            xls_book = xlwt.Workbook()
            for sheet_name in wb.sheetnames:
                xlsx_sheet = wb[sheet_name]
                xls_sheet = xls_book.add_sheet(sheet_name)
                for row_idx, row in enumerate(xlsx_sheet.iter_rows(values_only=False)):
                    for col_idx, cell in enumerate(row):
                        cell_value = cell.value
                        if cell_value is None:
                            continue
                        elif isinstance(cell_value, datetime):
                            try:
                                base_date = datetime(1900, 1, 1)
                                delta = cell_value - base_date
                                days_diff = delta.days
                                if cell_value >= datetime(1900, 3, 1):
                                    days_diff += 1
                                excel_date = days_diff + 1
                                if delta.seconds > 0 or delta.microseconds > 0:
                                    total_seconds = delta.seconds + (delta.microseconds / 1000000.0)
                                    excel_date += total_seconds / 86400.0
                                    date_style = xlwt.easyxf(num_format_str='YYYY-MM-DD HH:MM:SS')
                                else:
                                    date_style = xlwt.easyxf(num_format_str='YYYY-MM-DD')
                                xls_sheet.write(row_idx, col_idx, excel_date, date_style)
                            except Exception:
                                xls_sheet.write(row_idx, col_idx, str(cell_value))
                        elif isinstance(cell_value, (int, float)):
                            xls_sheet.write(row_idx, col_idx, cell_value)
                        elif isinstance(cell_value, bool):
                            xls_sheet.write(row_idx, col_idx, 1 if cell_value else 0)
                        else:
                            xls_sheet.write(row_idx, col_idx, str(cell_value))
            output = io.BytesIO()
            xls_book.save(output)
            output.seek(0)
            logger.info(f"Successfully converted XLSX to XLS format: {filename}")
            return output.getvalue()
        except Exception as e:
            error_msg = f"Error converting XLSX to XLS format: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if errors is not None:
                errors.append(error_msg)
            return None

    @staticmethod
    def is_xls_file(excel_file: FileModel) -> bool:
        """
        Checks if the file is an XLS file based on extension or magic bytes.
        
        Args:
            excel_file: FileModel containing the Excel file
            
        Returns:
            True if file is XLS format, False otherwise
        """
        filename_lower = excel_file.name.lower()
        if filename_lower.endswith('.xls') and not filename_lower.endswith('.xlsx'):
            return True
        if len(excel_file.content) >= 8 and excel_file.content[:4] == b'\xd0\xcf\x11\xe0':
            return True
        return False

    @staticmethod
    def convert_xls_to_xlsx(excel_file: FileModel, errors: Optional[List[str]] = None) -> Optional[Workbook]:
        """
        Converts an XLS file to an openpyxl Workbook.
        
        Args:
            excel_file: FileModel containing the XLS file
            errors: Optional list to append conversion errors
            
        Returns:
            openpyxl Workbook if conversion successful, None otherwise
        """
        if errors is None:
            errors = []
        try:
            xls_book = xlrd.open_workbook(file_contents=excel_file.content)
            sheet_names_list = xls_book.sheet_names() if callable(xls_book.sheet_names) else xls_book.sheet_names
            wb = Workbook()
            if len(sheet_names_list) > 0 and wb.active is not None:
                wb.remove(wb.active)
            for sheet_idx, sheet_name in enumerate(sheet_names_list):
                xls_sheet = xls_book.sheet_by_index(sheet_idx)
                ws = wb.create_sheet(title=sheet_name)
                for row_idx in range(xls_sheet.nrows):
                    for col_idx in range(xls_sheet.ncols):
                        cell_value = xls_sheet.cell_value(row_idx, col_idx)
                        cell_type = xls_sheet.cell_type(row_idx, col_idx)
                        if cell_type == xlrd.XL_CELL_DATE:
                            try:
                                if isinstance(cell_value, (int, float)):
                                    date_tuple = xlrd.xldate_as_tuple(cell_value, xls_book.datemode)
                                    cell_value = datetime(*date_tuple)
                                else:
                                    cell_value = str(cell_value)
                            except Exception:
                                cell_value = str(cell_value)
                        elif cell_type in (xlrd.XL_CELL_ERROR, xlrd.XL_CELL_EMPTY):
                            cell_value = None
                        ws.cell(row=row_idx + 1, column=col_idx + 1, value=cell_value)
            return wb
        except Exception as e:
            error_msg = f"Error converting XLS file to XLSX format: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if errors is not None:
                errors.append(error_msg)
            return None

    @staticmethod
    def load_excel_workbook(excel_file: FileModel, errors: Optional[List[str]] = None, 
                           read_only: bool = False, data_only: bool = True) -> Optional[Workbook]:
        """
        Loads an Excel file (XLS or XLSX) as an openpyxl Workbook.
        Automatically detects and converts XLS files to XLSX format.
        
        Args:
            excel_file: FileModel containing the Excel file
            errors: Optional list to append validation/conversion errors
            read_only: If True, opens workbook in read-only mode (only for XLSX)
            data_only: If True, loads only calculated values (only for XLSX)
            
        Returns:
            openpyxl Workbook if successful, None otherwise
        """
        if errors is None:
            errors = []
        if not excel_file.content or len(excel_file.content) == 0:
            error_msg = "Excel file is empty"
            logger.error(error_msg)
            if errors is not None:
                errors.append(error_msg)
            return None
        if ExcelUtilities.is_xls_file(excel_file):
            return ExcelUtilities.convert_xls_to_xlsx(excel_file, errors)
        else:
            try:
                return load_workbook(io.BytesIO(excel_file.content), read_only=read_only, data_only=data_only)
            except Exception as e:
                error_msg = f"Invalid Excel file format: {str(e)}"
                logger.error(error_msg, exc_info=True)
                if errors is not None:
                    errors.append(error_msg)
                return None
    
    @staticmethod
    def excel_to_tsv_files(excel_file: FileModel) -> list[FileModel]:
        """
        Converts an Excel file to multiple TSV files, one per sheet
        
        Args:
            excel_file: FileModel containing the Excel file
            
        Returns:
            List of FileModels, each containing a TSV file for one sheet
        """
        result = []
        
        # Load the Excel workbook from bytes
        wb = load_workbook(io.BytesIO(excel_file.content), read_only=True)
        
        # Process each sheet
        for sheet_name in wb.sheetnames:
            sheet = wb[sheet_name]
            
            # Create a TSV output buffer
            output = io.StringIO()
            tsv_writer = csv.writer(output, delimiter='\t')
            
            # Write each row to the TSV
            for row in sheet.rows:
                # Extract cell values from the row
                row_values = [cell.value if cell.value is not None else "" for cell in row]
                tsv_writer.writerow(row_values)
            
            # Create a FileModel with the TSV content
            tsv_filename = f"{sheet_name}.tsv"
            tsv_content = output.getvalue().encode('utf-8')
            
            result.append(FileModel(name=tsv_filename, content=tsv_content))
        
        return result

