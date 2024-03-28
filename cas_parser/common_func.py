import re
from locale import setlocale, LC_ALL, atof
import os
import csv

from .market_interface import Investor, Owner, DematAccount, FolioAccount

from enum import Enum

class ReaderStatus(Enum):
    Is_Finding = "Is Finding"
    Reading_Overview = "Reading Overview table"
    Reading_CDSL_Demat = "Reading CDSL Account Info"
    Reading_NSDL_Demat = "Reading NSDL Account Info"
    Reading_Folio = "Reading MF Account Info"
    Reading_Performance = "Reading Portfolio Performance"

class ReaderSubStatus(Enum):
    Is_Waiting = "Is Finding"
    Reading_Account_Info = "Reading Account Info"
    Reading_Transactions = "Reading Transactions"
    Reading_Holdings = "Reading Holdings"

def get_portfolio_account_summary(lines):
    owners = list()
    for line in lines:
        if isinstance(line, str):
            temp_line = line.replace(' ', '')
            if "(PAN:" in temp_line:
                # Found new owner
                new_owner = Owner()
                investors = line.split("\n")
                for inv in investors:
                    # Combination of ( PAN could differ with whitespace and extrac character
                    if "(" in inv and "PAN" in inv:
                        split_inv = inv.split(sep='(')
                        investor_name = split_inv[0]
                        investor_pan = split_inv[1]
                        investor_pan = "".join(e for e in investor_pan if e.isalnum() or e.isspace() )
                        investor_pan = re.split("PAN", investor_pan, flags=re.IGNORECASE)[1]
                        # This was commented and instead the above parsing logic has been added for I-Sec fix
                        # inv = "".join(e for e in inv if e.isalnum() or e.isspace() )
                        # [name, pan_no] = re.split("PAN", inv, flags=re.IGNORECASE)
                        new_owner.investors.append(Investor(investor_name.strip(), investor_pan.strip()))
                owners.append(new_owner)

        if isinstance(line, list):
            if line[0] == "CDSL Demat Account" or line[0] == "NSDL Demat Account":
                acc = process_demat_account_row(line)
                if acc:
                    # TODO: Check if the following field is required
                    # acc.as_on_date = as_on_date
                    new_owner.demat_accounts.append(acc)

            elif line[0] == "Mutual Fund Folios":
                # TODO: think about which number to track.
                # Folio Account count or ISIN count
                setlocale(LC_ALL, '')
                # Locale does not work properly on windows
                sl = line[1].split("Folio")
                new_owner.folio_count = int(sl[0])
                new_owner.folio_value = atof(line[3])
    
    return owners

def get_valid_cell_count(row):
    valid_cell_count = 0
    for cell in row:
        if cell:            
            valid_cell_count += 1
    return valid_cell_count

def is_valid_line(line: str, skip_line_list):
    # All the conditions that will make line invalid
    fail_conditions = [
        not bool(re.match('[A-Za-z0-9]', line)),
        "Page" in line and "of" in line,
        line in skip_line_list
    ]

    # check if any one fail condition is satisfied.
    return not (True in fail_conditions)
    
def remove_non_ascii_words_from_string(line: str):
    #This function will remove non ASCII characters from given string.
    #input: 'खा�याचा उप-�कार: Individual- Resident Negative Nomination'
    #output: '-: Individual- Resident Negative Nomination'
    test_list = line.split(' ')
    final = []
    for item in test_list:
        if item:
            ele = re.search(r'[^\x00-\x7F]', item)
            # take only complete words
            if not ele:
                final.append(item)

    result = " ".join(final)
    return result

def remove_non_ascii_letters_from_string(line: str):
    # This function will remove non ASCII characters from given string.
    # input: 'खा�याचा उप-�कार: Individual- Resident Negative Nomination'
    # output: '-: Individual- Resident Negative Nomination'
    
    result = None
    if line:
        ret_val = re.sub(r'[^\x00-\x7F]', '', line).strip()
        result =  re.sub(' +', ' ', ret_val)
    return result

def clean_pdf_data(raw_content, skip_lines, start_token_line):
    clean_content = list()

    start_found= False
    for index, data in raw_content.items():

        if data["type"] == "line":
            actual_line = " ".join(data["data"])
            line = remove_non_ascii_words_from_string(actual_line)

            if line == start_token_line and not start_found:
                start_found = True

            if start_found and is_valid_line(line, skip_lines):
                clean_content.append(line)
            
        elif data["type"] == "table":
            for row in data["data"]:
                valid_cells = get_valid_cell_count(row)

                # If there are no valid cells in the row, it can be treated as an empty line and could be skipped.    
                if valid_cells == 0:
                    continue

                # if there is only one valid cell in the row, it should be treated as a line.
                elif valid_cells == 1:
                    actual_line = " ".join(filter(None, row))
                    line = remove_non_ascii_letters_from_string(actual_line)

                    if is_valid_line(line, skip_lines):
                        clean_content.append(line)

                # else save in the content
                else:
                    actual_row = list()
                    for cell in row:
                        new_cell = remove_non_ascii_letters_from_string(cell)
                        actual_row.append(new_cell)
                    clean_content.append(actual_row)

    return clean_content

# def clean_pdf_data(raw_content, skip_lines, start_token_line):
#     clean_content = list()

#     start_found= False
#     for index, data in raw_content.items():

#         if data["type"] == "line":
#             line = " ".join(data["data"])

#             if line == start_token_line and not start_found:
#                 start_found = True

#             if start_found and is_valid_line(line, skip_lines):
#                 clean_content.append(line)
            
#         elif data["type"] == "table":
#             for row in data["data"]:
#                 valid_cells = get_valid_cell_count(row)

#                 # If there are no valid cells in the row, it can be treated as an empty line and could be skipped.    
#                 if valid_cells == 0:
#                     continue

#                 # if there is only one valid cell in the row, it should be treated as a line.
#                 elif valid_cells == 1:
#                     line = " ".join(filter(None, row))

#                     if is_valid_line(line, skip_lines):
#                         clean_content.append(line)

#                 # else save in the content
#                 else:
#                     clean_content.append(row)

#     return clean_content

# def get_valid_cell_count(row):
#     valid_cell_count = 0
#     for cell in row:
#         if cell:            
#             valid_cell_count += 1
#     return valid_cell_count

# def is_valid_line(line, skip_line_list):
#     is_valid = True
    
#     if "Page" in line and "of" in line:
#         is_valid = False
        
#     if line in skip_line_list:
#         is_valid = False
        
#     return is_valid
            
def make_clean_segments(line, tokens, no_special_character = True, ignore_case = True):
    segments = list()

    if no_special_character:
        line = "".join(e for e in line if e.isalnum() or e.isspace() )

    case_flag = re.IGNORECASE if ignore_case else 0

    new_line = None
    for token_idx, token in enumerate(tokens):
        if not new_line:
            new_line = line

        sl = re.split(token, new_line, flags=case_flag)
        new_line = sl[1]

        if token_idx != 0:
            segments.append(sl[0].strip())
        if token_idx == len(tokens) - 1:
            segments.append(sl[1].strip())

    return segments

def get_demat_owner_name(row):
    investors = []
    new_line = ' '.join(filter(None, row))
    sl = new_line.split('\n')
    for temp in sl:
        if "PAN" in temp:
            temp = "".join(e for e in temp if e.isalnum() or e.isspace() )
            sl2 = re.split("PAN", temp, flags=re.IGNORECASE)
            inv = Investor()
            inv.name = sl2[0].strip()
            inv.PAN = sl2[1].strip()
            investors.append(inv)

    return investors

def process_demat_account_row(row):
    # Locale does not work properly on windows
    setlocale(LC_ALL, '')

    acc = DematAccount()
    acc.depository = row[0].strip()
    sl = row[1].split("\n")
    last_sl = sl.pop(-1)
    acc.dp_name = " ".join(sl).strip()
    [acc.dp_id, acc.client_id] = make_clean_segments(last_sl, ["DP Id", "Client Id"])
    acc.security_count = int(row[2])
    acc.security_value_in_rs = atof(row[3])

    return acc

def to_valid_number(s: str):
    if s is None:
        return 0.0
    try:
        return atof(s)
    except ValueError:
        return 0.0

def get_clean_row(row):
    real_cells = list()
    for cell in row:
        if cell is not None and cell != "":
            real_cells.append(cell)
    return real_cells

def check_for_empty_table(rows):
    is_empty = False
    if len(rows) > 0:
        if rows[0][0] == 'No Transaction during the period':
            is_empty = True
        elif rows[0] == 'Nil Holding':
            is_empty = True

    return is_empty

def get_lines_from_content(content, index, total_count):
    skip_indices = list()
    dp_info_lines = list()

    for i in range(total_count):
        new_idx = index + i
        data = content[new_idx]
        dp_info_lines.append(" ".join(data["data"]))
        skip_indices.append(new_idx)

    return dp_info_lines, skip_indices

def save_csv(file_path, header, rows):
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)
    with open(file_path, "w", newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)

if __name__ == "__main__":
    header = ["1", "2"]
    rows = [
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        [1, 2, 3, 4],
    ]
    save_csv("C:\_Finalyca\_finalyca\pdf_scrapper\\test.csv", header, rows )