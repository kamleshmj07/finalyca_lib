
class MissingDataException(Exception):
    def __init__(self, info):
        self.info = info

    def __str__(self):
        return self.info