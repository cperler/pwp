from models import *
import datetime

def partition(lst, n):
    division = len(lst) / float(n)
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in xrange(n) ]

def batch_retrieve_days_prices():
    tickers = [stock.ticker for stock in Pwp_Stock.select()]
    today = datetime.date.today()
    for lst in partition(tickers, len(tickers) / 100):
        eod_quotes = get_eod_quotes(lst, today)
        for eod_quote in eod_quotes:
            try:
                symbol = eod_quote['Security']['Symbol']
                last_close = eod_quote['LastClose']
                stock = Pwp_Stock.select().where(Pwp_Stock.ticker == symbol).get()
                eod_quote_for_date = Pwp_Stock_Price_History.select().where((Pwp_Stock_Price_History.stock==stock.stock) & (Pwp_Stock_Price_History.date==today))

                if eod_quote_for_date.count() == 0:
                    if last_close == 0.0:
                        print 'Unable to retrieve px for (%s, %s) -- not persisting.' % (stock.ticker, today)
                    else:
                        Pwp_Stock_Price_History.create(close=last_close, date=today, stock=stock.stock)
                        print 'Persisted px for (%s, %s).' % (stock.ticker, today)
                else:
                    print 'Not persisting px for (%s, %s) -- already exist.' % (symbol, today)
            except Exception, e:
                print 'Error retrieving quote: %s' % eod_quote['Message']

batch_retrieve_days_prices()