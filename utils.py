from passwords import env
import urls
from urls import *

xignite_token = env['xignite']['token']

def get_exchanges():
    return request(list_exchanges % xignite_token)

def get_sectors():
    return request(list_sectors % xignite_token)

def get_industries():
    return request(list_industries % xignite_token)

def get_instrument_details(identifier, identifier_type, start, end):
    return request(get_instrument % (identifier, identifier_type, start, end, xignite_token))

def get_instruments_details(identifier_list, identifier_type, as_of):
    return request(get_instruments % (','.join(identifier_list), identifier_type, as_of, xignite_token))

def get_eod_quotes(identifier_list, identifier_type, as_of):
    return request(get_eod_quotes_for_date % (','.join(identifier_list), identifier_type, as_of, xignite_token))
                        
def get_last_closing_prices(identifier_list, identifier_type):
    return request(urls.get_last_closing_prices % (','.join(identifier_list), identifier_type, xignite_token))
