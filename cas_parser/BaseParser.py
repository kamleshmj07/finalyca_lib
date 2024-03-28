from datetime import date
from typing import List, Dict
import json

from .market_interface import Statement
from .pdf_helper import parse_pdf

class BaseParser():
    def __init__(self) -> None:
        self.statement : Statement = Statement()
        self.as_on_date : date = None

    def process(self, content: Dict) -> Statement:
        # Do not implement this method. it has to be implemented by the inherited class
        raise NotImplementedError()

    def parse(self, file_path, password : str = None, include_pages : List[int] = None, exclude_pages : List[int] = None):
        content = parse_pdf(file_path, password, include_pages, exclude_pages)
        return self.process(content)
    
    def export_content(self, pdf_file_path, json_file_path, password : str = None, include_pages : List[int] = None, exclude_pages : List[int] = None):
        content = parse_pdf(pdf_file_path, password, include_pages, exclude_pages)

        with open(json_file_path, 'w') as file:
            json.dump(content, file)

    def import_content(self, json_file_path):
        content = dict()
        with open(json_file_path) as file:
            content = json.load(file)
        return self.process(content)