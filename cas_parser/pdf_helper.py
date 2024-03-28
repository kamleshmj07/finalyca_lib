import pdfplumber
from typing import List

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

import json
class Box:
    def __init__(self, left_x, top_y, right_x, bottom_y):
        # Top Left point
        self.tl = Point(left_x, top_y)
        # Bottom Right point
        self.br = Point(right_x, bottom_y)
    
    @classmethod
    def from_word(cls, word):
        return cls(word["x0"], word["top"], word["x1"], word["bottom"])
        
    def __str__(self):
        return json.dumps({"left": round(self.tl.x, 3), "top": round(self.tl.y, 3), "right": round(self.br.x, 3), "bottom": round(self.br.y,3) })
    
    def is_overlapped(self, box: 'Box'):
        '''
        Here it is assumed that top left point is (0,0). From top to bottom, y axis increases (inverse to conventional cartesian y axis)
        X axis follows the same pattern where it increases from left to right.
        '''
        if (self.tl.x > box.br.x) or (box.tl.x > self.br.x):
            return False
        
        if (self.tl.y > box.br.y) or (box.tl.y > self.br.y):
            return False
        
        return True

def parse_pdf(pdf_file_path, password = "", include_pages : List[int] = list(), exclude_pages : List[int] = list()):
    content = dict()

    with pdfplumber.open(pdf_file_path, password = password) as pdf:
        index = 0

        for page_no, page in enumerate(pdf.pages):
            scan_page = True

            if include_pages and page_no not in include_pages:
                scan_page = False
            
            if exclude_pages and page_no in exclude_pages:
                scan_page = False

            if scan_page:
                index, page_content = parse_single_pdf_page(index, page)
                content.update(page_content)
    
    return content

def parse_single_pdf_page(index, page: pdfplumber.pdf.Page):
    page = page.dedupe_chars()
    words = page.extract_words(use_text_flow=True)
    tables = page.find_tables()

    table_boxes = []
    for table in tables:
        table_box = Box(*table.bbox)
        table_boxes.append(table_box)

    content = dict()
    current_line_top = None
    current_table_index = None

    # We are assuming that we have lines and tables in vertical manner. there will not be any words outside of the table.
    for word in words:
        is_line = True
        word_box = Box.from_word(word)
        table_index = None
        for idx, t_box in enumerate(table_boxes):
            if t_box.is_overlapped(word_box):
                is_line= False
                table_index = idx

        if is_line:
            new_line = False
            current_word_top = round(word["top"], 3)
            if not current_line_top:
                new_line = True
            else:
                if abs(current_word_top - current_line_top) > 10:
                    new_line = True

            if new_line:
                index = index + 1
                current_line_top = current_word_top
                content[index] = {"type": "line", "data": list()}

            content[index]["data"].append(word["text"])

        else:
            new_table_found = False
            is_table_valid = True

            if current_table_index == None: 
                current_table_index = table_index
                new_table_found = True
            
            if current_table_index != table_index:
                current_table_index = table_index
                new_table_found = True

            # SKIP the header table
            current_table = tables[current_table_index]
            (x0, top, x1, bottom) = current_table.bbox
            if x0 == 0.0 and top == 0.0:
                is_table_valid = False
            
            if new_table_found and is_table_valid:
                index = index + 1
                content[index] = {"type": "table", "data": current_table.extract()}

    return index, content

