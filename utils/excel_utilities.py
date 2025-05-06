import io
import csv
from openpyxl import load_workbook
from models.file_model import FileModel


class ExcelUtilities:
    """
    Utility class for Excel operations
    """
    
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