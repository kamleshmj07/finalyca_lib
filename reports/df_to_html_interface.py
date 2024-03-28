import enum

class currency_type(enum.Enum):
    USD = 'USD'
    INR = 'INR'
    EUR = 'EUR'

class df_to_html_column_type(enum.Enum):
    Number = "number"
    Percent = "percent"
    Money = "money"
    String = "string"
    Decimal = "decimal"

class ReportTableColumnInfo:
    def __init__(self, name, label, type, width_in_percent, currency_type=None) -> None:
        self.name = name
        self.label = label
        self.type = type
        self.width_in_percent = width_in_percent
        self.currency_type = currency_type
        # pass
