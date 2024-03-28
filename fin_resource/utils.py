from datetime import datetime

def prettify(text: str):
    '''
    First remove _id if there. Then split text at '_'and makes first letter of every word in upper case
    '''
    n_text = text.replace("_id", "")
    return " ".join(n_text.split("_")).title()

def codify(text:str):
    '''
    replace text at ' ' and '-' with '_' and make everything lower
    '''
    return text.replace(" ", "_").replace("-", "_").lower()

def get_current_ts() -> datetime:
    """ Get current datetime object
    :returns: current datetime object
    """
    return datetime.now()