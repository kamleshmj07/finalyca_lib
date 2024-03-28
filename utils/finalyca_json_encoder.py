import flask
import datetime
import decimal
import logging
from flask import current_app

class FinalycaJSONEncoder(flask.json.JSONEncoder):
    '''
    Used to provide json conversion based on Finalyca requirements. 
    Does not work for basic types (str, bool, float, int etc)
    '''
    def default(self, obj):
        # if type(obj) is float:
        #     return float(round(obj, 2)) if obj else None
        try:
            if isinstance(obj, datetime.datetime):
                return obj.strftime('%d %b %Y') if obj else None

            elif isinstance(obj, datetime.date):
                return obj.strftime('%d %b %Y') if obj else None

            elif isinstance(obj, decimal.Decimal):
                return float(round(obj, 2)) if obj else None

            return super().default(obj)
        
        except Exception as ex:
            current_app.logger.error(F"{obj} was not converted.")
            current_app.logger.exception(ex)

    # def default(self, obj):
    #     # if type(obj) is float:
    #     #     return float(round(obj, 2)) if obj else None
    #     current_app.logger.warning(F"converting {obj} object.")

    #     if isinstance(obj, datetime.datetime):
    #         return obj.strftime('%d %b %Y') if obj else None

    #     elif isinstance(obj, datetime.date):
    #         return obj.strftime('%d %b %Y') if obj else None

    #     elif isinstance(obj, decimal.Decimal):
    #         return float(round(obj, 2)) if obj else None

    #     return super().default(obj)

