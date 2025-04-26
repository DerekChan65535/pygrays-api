import logging

from models.file_model import FileModel
from services.multi_logging import LoggingService
import itertools
import os, re, csv, json
from openpyxl import Workbook
import decimal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class InventoryService:

    def __init__(self):
        pass



    def process_inventory_request(self, files: list[FileModel]):
        logger.info("Processing inventory request")

        #Create a empty excel workbook
        workbook = Workbook()


        dropship_sales_files = sorted([x for x in files if re.match(r'^DropshipSales\d{8}\.txt$', x.name)], key=lambda x: x.name)






        return [f"File {file.name} has {len(file.content)} bytes" for file in files]
