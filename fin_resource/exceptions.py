class MissingFieldException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class FieldMutationException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class FieldIntegrityException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class InvalidSelectionException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class UnknownFieldException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class NotSupportedException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info

class MissingInfoException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info
    
class NotUniqueValueException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info