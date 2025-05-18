# Schema Configurations for PyGrays API

from typing import Dict, List, Any, Type, Optional, Union
import csv
import io
import logging
import re
from datetime import datetime
import decimal
from openpyxl import Workbook, worksheet

logger = logging.getLogger(__name__)

class SchemaField:
    def __init__(self, field_type: str, required: bool = False, formats: Optional[List[str]] = None, number_format: Optional[str] = None):
        self.field_type = field_type
        self.required = required
        self.formats = formats or []
        self.number_format = number_format

    def convert(self, value: Any) -> Any:
        if not value:
            return None if not self.required else value
        try:
            if self.field_type == 'datetime':
                return self._parse_date(value)
            elif self.field_type == 'float':
                return float(value)
            elif self.field_type == 'integer':
                return int(value)
            elif self.field_type == 'boolean':
                return value.upper() in ['TRUE', 'YES', 'Y', '1'] if isinstance(value, str) else bool(value)
            elif self.field_type == 'decimal':
                cleaned_value = re.sub(r'[^\d.]', '', str(value))
                return decimal.Decimal(cleaned_value)
            return value
        except (ValueError, TypeError, decimal.InvalidOperation) as e:
            logger.warning(f'Conversion error for value {value} to type {self.field_type}: {str(e)}')
            return value

    def _parse_date(self, date_string: str) -> Optional[datetime]:
        if not date_string:
            return None
        for fmt in self.formats:
            try:
                return datetime.strptime(date_string, fmt)
            except ValueError:
                continue
        logger.warning(f'Failed to parse date {date_string} with formats {self.formats}')
        return None

class BaseSchema:
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema

class ImportSchema(BaseSchema):
    def import_data(self, raw_data: bytes, errors: List[str]) -> List[Dict[str, Any]]:
        try:
            text = raw_data.decode('utf-8-sig')
            reader = csv.DictReader(io.StringIO(text))
            data = []
            row_count = 0
            conversion_errors = 0

            for row in reader:
                row_count += 1
                converted_row = {}
                row_errors = 0

                for field, value in row.items():
                    if field not in self.schema:
                        converted_row[field] = value
                        continue

                    field_schema = self.schema[field]
                    if not value and field_schema.required:
                        logger.warning(f'Missing required field {field} in row {row_count}')
                        row_errors += 1
                    converted_row[field] = field_schema.convert(value) if value else None

                if row_errors > 0:
                    conversion_errors += 1
                data.append(converted_row)

            logger.info(f'Imported {row_count} rows, with {conversion_errors} conversion errors')
            return data
        except Exception as e:
            errors.append(f'Error importing data: {str(e)}')
            logger.error(f'Error importing data', exc_info=True)
            return []

class ExportSchema(BaseSchema):
    def export_data(self, data: List[Dict[str, Any]], workbook: Workbook, sheet_name: str, errors: List[str]) -> bool:
        try:
            sheet = workbook.create_sheet(sheet_name)
            headers = list(self.schema.keys())
            sheet.append(headers)

            for row_idx, item in enumerate(data, start=2):
                row_values = []
                for col_idx, col in enumerate(headers, start=1):
                    value = item.get(col, '')
                    if isinstance(value, decimal.Decimal):
                        try:
                            value = value.quantize(decimal.Decimal('0.01'), rounding=decimal.ROUND_HALF_UP)
                        except (decimal.InvalidOperation, TypeError):
                            value = ''
                    row_values.append(value)
                    # Apply number format if specified in the schema
                    if col in self.schema and self.schema[col].number_format:
                        sheet.cell(row=row_idx, column=col_idx).number_format = self.schema[col].number_format
                sheet.append(row_values)

            logger.info(f'Exported {len(data)} rows to {sheet_name} sheet')
            return True
        except Exception as e:
            errors.append(f'Error exporting data to {sheet_name}: {str(e)}')
            logger.error(f'Error exporting data to {sheet_name}', exc_info=True)
            return False

# Aging Report Service Schemas
aging_report_daily_data_import_schema = ImportSchema({
    'Classification': SchemaField('string'),
    'Sale_No': SchemaField('string', required=True),
    'Description': SchemaField('string'),
    'Division': SchemaField('string', required=True),
    'BDM': SchemaField('string'),
    'Sale_Date': SchemaField('datetime', formats=['%d/%m/%Y %H:%M', '%d/%m/%Y %I:%M:%S %p', '%d/%m/%Y']),
    'Gross_Tot': SchemaField('float', required=True),
    'Delot_Ind': SchemaField('boolean'),
    'Cheque_Date': SchemaField('datetime', formats=['%d/%m/%Y %H:%M', '%d/%m/%Y %I:%M:%S %p', '%d/%m/%Y']),
    'Day0': SchemaField('float'),
    'Day1': SchemaField('float'),
    'Day2': SchemaField('float'),
    'Day3': SchemaField('float'),
    'Day4': SchemaField('float'),
    'Day5': SchemaField('float'),
    'Day6': SchemaField('float'),
    'Day7': SchemaField('float'),
    'Day8': SchemaField('float'),
    'Day9': SchemaField('float'),
    'Day10': SchemaField('float'),
    'Day11': SchemaField('float'),
    'Day12': SchemaField('float'),
    'Day13': SchemaField('float'),
    'Day14': SchemaField('float'),
    'Day15': SchemaField('float'),
    'Day16': SchemaField('float'),
    'Day17': SchemaField('float'),
    'Day18': SchemaField('float'),
    'Day19': SchemaField('float'),
    'Day20': SchemaField('float'),
    'Day21': SchemaField('float'),
    'Day22': SchemaField('float'),
    'Day23': SchemaField('float'),
    'Day24': SchemaField('float'),
    'Day25': SchemaField('float'),
    'Day26': SchemaField('float'),
    'Day27': SchemaField('float'),
    'Day28': SchemaField('float'),
    'Day29': SchemaField('float'),
    'Day30': SchemaField('float'),
    'Day31': SchemaField('float'),
})

aging_report_export_schema = ExportSchema({
    'Classification': SchemaField('string'),
    'Sale_No': SchemaField('string'),
    'Description': SchemaField('string'),
    'Division': SchemaField('string'),
    'BDM': SchemaField('string'),
    'Sale_Date': SchemaField('datetime'),
    'Gross_Tot': SchemaField('float'),
    'Delot_Ind': SchemaField('boolean'),
    'Cheque_Date': SchemaField('datetime'),
    'Day0': SchemaField('float'),
    'Day1': SchemaField('float'),
    'Day2': SchemaField('float'),
    'Day3': SchemaField('float'),
    'Day4': SchemaField('float'),
    'Day5': SchemaField('float'),
    'Day6': SchemaField('float'),
    'Day7': SchemaField('float'),
    'Day8': SchemaField('float'),
    'Day9': SchemaField('float'),
    'Day10': SchemaField('float'),
    'Day11': SchemaField('float'),
    'Day12': SchemaField('float'),
    'Day13': SchemaField('float'),
    'Day14': SchemaField('float'),
    'Day15': SchemaField('float'),
    'Day16': SchemaField('float'),
    'Day17': SchemaField('float'),
    'Day18': SchemaField('float'),
    'Day19': SchemaField('float'),
    'Day20': SchemaField('float'),
    'Day21': SchemaField('float'),
    'Day22': SchemaField('float'),
    'Day23': SchemaField('float'),
    'Day24': SchemaField('float'),
    'Day25': SchemaField('float'),
    'Day26': SchemaField('float'),
    'Day27': SchemaField('float'),
    'Day28': SchemaField('float'),
    'Day29': SchemaField('float'),
    'Day30': SchemaField('float'),
    'Day31': SchemaField('float'),
    'State': SchemaField('string'),
    'State-Division Name': SchemaField('string'),
    'Payment Days': SchemaField('integer'),
    'Due Date': SchemaField('datetime', number_format='DD-MMM-YY'),
    'Division Name': SchemaField('string'),
    'Sub Division Name': SchemaField('string'),
    'Gross Amount': SchemaField('float', number_format='_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_'),
    'Collected': SchemaField('float', number_format='_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_'),
    'To be Collected': SchemaField('float', number_format='_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_'),
    'Payable to Vendor': SchemaField('float', number_format='_(* #,##0.00_);_(* (#,##0.00);_(* "-"??_);_(@_'),
    'Month': SchemaField('string'),
    'Year': SchemaField('integer'),
    'Cheque Date Y/N': SchemaField('string'),
    'Days Late for Vendors Pmt': SchemaField('integer'),
    'Comments': SchemaField('string')
})

# Inventory Service Schemas
inventory_dropship_sales_schema = ImportSchema({
    'Customer': SchemaField('string'),
    'AX_ProductCode': SchemaField('string'),
    'GST': SchemaField('string'),
    'Units': SchemaField('integer'),
    'Price': SchemaField('decimal'),
    'Amount': SchemaField('decimal'),
    'SaleNo': SchemaField('string'),
    'VendorNo': SchemaField('string'),
    'ItemNo': SchemaField('string'),
    'Description': SchemaField('string'),
    'Serial_No': SchemaField('string'),
    'Vendor_Ref_No': SchemaField('string'),
    'DropShipper': SchemaField('string'),
    'Consignment': SchemaField('string'),
    'DealNo': SchemaField('string'),
    'Column1': SchemaField('string'),
    'BP': SchemaField('decimal'),
    'SaleType': SchemaField('string'),
    'FreightCodeDescription': SchemaField('string')
})

inventory_deals_schema = ImportSchema({
    'Customer': SchemaField('string'),
    'AX_ProductCode': SchemaField('string'),
    'GST': SchemaField('string'),
    'Units': SchemaField('integer'),
    'Price': SchemaField('decimal'),
    'Amount': SchemaField('decimal'),
    'SaleNo': SchemaField('string'),
    'VendorNo': SchemaField('string'),
    'ItemNo': SchemaField('string'),
    'Description': SchemaField('string'),
    'Serial_No': SchemaField('string'),
    'Vendor_Ref_No': SchemaField('string'),
    'DropShipper': SchemaField('string'),
    'Consignment': SchemaField('string'),
    'DealNo': SchemaField('string'),
    'Column1': SchemaField('string'),
    'BP': SchemaField('decimal'),
    'SaleType': SchemaField('string'),
    'DivisionCode': SchemaField('string'),
    'DivisionDescription': SchemaField('string'),
    'FreightCodeDescription': SchemaField('string')
})

inventory_uom_mapping_schema = ImportSchema({
    'Item': SchemaField('string'),
    'UOM': SchemaField('decimal')
})

inventory_mixed_export_schema = ExportSchema({
    'Customer': SchemaField('string'),
    'AX_ProductCode': SchemaField('string'),
    'Per_Unit_Cost': SchemaField('decimal'),
    'Units': SchemaField('integer'),
    'Price': SchemaField('decimal'),
    'Amount': SchemaField('decimal'),
    'SaleNo': SchemaField('string'),
    'VendorNo': SchemaField('string'),
    'ItemNo': SchemaField('string'),
    'Description': SchemaField('string'),
    'Serial_No': SchemaField('string'),
    'COGS': SchemaField('decimal'),
    'SALE_EX_GST': SchemaField('decimal'),
    'BP_EX_GST': SchemaField('decimal'),
    'Vendor_Ref_No': SchemaField('string'),
    'DropShipper': SchemaField('string'),
    'Consignment': SchemaField('string'),
    'DealNo': SchemaField('string'),
    'Column1': SchemaField('string'),
    'BP': SchemaField('decimal'),
    'SaleType': SchemaField('string'),
    'FreightCodeDescription': SchemaField('string')
})

inventory_wine_export_schema = ExportSchema({
    'Customer': SchemaField('string'),
    'AX_ProductCode': SchemaField('string'),
    'Per_Unit_Cost': SchemaField('decimal'),
    'Units': SchemaField('integer'),
    'Price': SchemaField('decimal'),
    'Amount': SchemaField('decimal'),
    'SaleNo': SchemaField('string'),
    'VendorNo': SchemaField('string'),
    'ItemNo': SchemaField('string'),
    'Description': SchemaField('string'),
    'Serial_No': SchemaField('string'),
    'COGS': SchemaField('decimal'),
    'SALE_EX_GST': SchemaField('decimal'),
    'BP_EX_GST': SchemaField('decimal'),
    'Vendor_Ref_No': SchemaField('string'),
    'DropShipper': SchemaField('string'),
    'Consignment': SchemaField('string'),
    'DealNo': SchemaField('string'),
    'Column1': SchemaField('string'),
    'BP': SchemaField('decimal'),
    'SaleType': SchemaField('string'),
    'DivisionCode': SchemaField('string'),
    'DivisionDescription': SchemaField('string'),
    'FreightCodeDescription': SchemaField('string')
}) 