from .common_enum import FilterType

class ExactFilter:
    def __init__(self, value) -> None:
        self.type = FilterType.exact

        self.value = value

class TextFilter:
    def __init__(self, value) -> None:
        self.type = FilterType.partial

        self.search = value

class RangeFilter:
    def __init__(self, value_1, value_2) -> None:
        self.type = FilterType.range

        self.min = value_1 if value_1 < value_2 else value_2 
        self.max = value_2 if value_1 < value_2 else value_1

class ChoiceFilter:
    def __init__(self, values) -> None:
        self.type = FilterType.choice

        self.choices = values