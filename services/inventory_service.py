from services.multi_logging import LoggingService
import itertools
import os, re, csv, json
from openpyxl import Workbook
import decimal


class FileModel:
    def __init__(self, name: str, content: bytes):
        self.name = name
        self.content = content


class InventoryService:

    def __init__(self, logging_service: LoggingService):
        self.logger = logging_service.get_logger(__name__)

    def process_inventory_request(self, files: list[FileModel]):
        self.logger.info(f"Processing inventory request for {len(files)} files")

        dropship_sales_file = [x for x in files if re.match(r'^DropshipSales\d{8}\.txt$', x.name)]








        return [f"File {file.name} has {len(file.content)} bytes" for file in files]
