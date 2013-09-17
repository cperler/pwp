import peewee
from models import *
from utils import *
from tasks import *

def test_get_price():
    print Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().get_last_closing_price()
    print Pwp_Stock.select().where(Pwp_Stock.stock == 2384).get().get_last_closing_price()
    print Pwp_Stock.select().where(Pwp_Stock.stock == 3416).get().get_last_closing_price()

def test_get_eod():
    print Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().get_eod_quote_for_date()
    print Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().get_eod_quote_for_date(datetime.date(2013, 07, 29))
    print Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().get_eod_quote_for_date(datetime.date(2013, 01, 24))
    print Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().get_eod_quote_for_date(datetime.date(2013, 01, 24), 'Symbol')

def test_update_px_history():
    ibm = Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().stock
    px_history = Pwp_Stock_Price_History.delete().where((Pwp_Stock_Price_History.stock==ibm) & (Pwp_Stock_Price_History.date==datetime.date(2013, 07, 29)))
    px_history.execute()
    Pwp_Stock.select().where(Pwp_Stock.ticker == 'IBM').get().update_eod_quote_for_date(datetime.date(2013, 07, 29))

def test_get_static():
    print get_exchanges()
    print get_sectors()
    print get_industries()

def test_get_instrument():
    print get_instrument_details('GOOG', 'Symbol', datetime.date(2013,07,29), datetime.date(2013,07,30))
    print get_instrument_details('US38259P5089', 'ISIN', datetime.date(2013, 07, 29), datetime.date(2013, 07, 30))
    print get_instruments_details(['GOOG', 'IBM'], 'Symbol', datetime.date(2013, 07, 29))
    print get_instruments_details(['US38259P5089', 'US4592001014'], 'ISIN', datetime.date(2013, 07, 29))

def test_get_eod_quotes():
    print get_eod_quotes(['GOOG', 'IBM'], 'Symbol', datetime.date(2013,07,29))
    print get_eod_quotes(['US38259P5089', 'US4592001014'], 'ISIN', datetime.date(2013, 07, 29))

def test_get_last_closing_prices():
    print get_last_closing_prices(['GOOG', 'IBM'], 'Symbol')

def test_partition():
    a = [1,2,3,4,5,6,7,8,9]
    print partition(a, 4)

#test_get_price()
#test_get_eod()
#test_update_px_history()
#test_get_static()
#test_get_instrument()
test_get_eod_quotes()
test_get_last_closing_prices()
#test_partition()
