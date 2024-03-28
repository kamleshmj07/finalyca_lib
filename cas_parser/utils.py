import re

def find_date_using_regex(input_text):
    #This function will return first date from given string which will match date patter i.e 31-Dec-2023
    if input_text:
        pattern = re.compile(r"([A-Za-z0-9]+(-[A-Za-z0-9]+)+)", re.IGNORECASE)
        return pattern.match(input_text).group(0)
    
    return None

def remove_non_ascii_from_string(input_text):
    #This function will remove non ASCII characters from given string.
    #input: 'खा�याचा उप-�कार: Individual- Resident Negative Nomination'
    #output: '-: Individual- Resident Negative Nomination'

    if input_text:
        ret_val = re.sub(r'[^\x00-\x7F]', '', input_text).strip()
        return re.sub(' +', ' ', ret_val)
    
    return None