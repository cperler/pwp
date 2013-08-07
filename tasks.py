from models import *
from utils import *
import datetime

def partition(lst, n):
    division = len(lst) / float(n)
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in xrange(n) ]
    
def batch_retrieve_days_prices():
    isins = [stock.isin for stock in Pwp_Stock.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.ticker) for stock in Pwp_Stock.select() if stock.isin is None]
    print 'Updating %s stocks that have isins.' % len(isins)

    success = []
    skips = []
    errors = []

    today = datetime.date.today()
    for lst in partition(isins, len(isins) / 100):
        eod_quotes = get_eod_quotes(lst, 'ISIN', today)
        for eod_quote in eod_quotes:
            try:
                symbol = eod_quote['Security']['Symbol']
                isin = eod_quote['Security']['ISIN']
                last_close = eod_quote['LastClose']
                stock = Pwp_Stock.select().where(Pwp_Stock.isin == isin).get()
                eod_quote_for_date = Pwp_Stock_Price_History.select().where((Pwp_Stock_Price_History.stock==stock.stock) & (Pwp_Stock_Price_History.date==today))

                if eod_quote_for_date.count() == 0:
                    if last_close == 0.0:
                        print 'Unable to retrieve px for (%s, %s) -- not persisting.' % (stock.ticker, today)
                        errors.append((symbol, isin))
                    else:
                        Pwp_Stock_Price_History.create(close=last_close, date=today, stock=stock.stock)
                        success.append((symbol, isin))
                        print 'Persisted px for (%s, %s).' % (stock.ticker, today)
                else:
                    skips.append((symbol, isin))
                    print 'Not persisting px for (%s, %s, %s) -- already exists.' % (symbol, isin, today)
            except Exception, e:
                errors.append((symbol, isin))
                print 'Error retrieving quote: %s' % eod_quote['Message']

    print 'Successfully updated prices for %s stocks.' % len(success)
    print 'Skipped updating %s stocks as they were already current.' % len(skips)
    print 'Found errors updating %s stocks.' % len(errors)
    print 'ISINs missing on %s stocks.' % len(no_isins)

def csv():
    isins = [stock.isin for stock in Pwp_Stock.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.ticker) for stock in Pwp_Stock.select() if stock.isin is None]
    print 'Updating %s stocks that have isins.' % len(isins)

    success = []
    errors = []
    
    today = datetime.date.today()
    f = open('out.csv', 'w')
    f.write('market,ticker,id,previous_close,latest_close,native currency,industry sector,isin\n')
    for lst in partition(isins, len(isins) / 100):
        eod_quotes = get_eod_quotes(lst, 'ISIN', today)
        for eod_quote in eod_quotes:
            try:
                market = eod_quote['Security']['Market']
                symbol = eod_quote['Security']['Symbol']
                isin = eod_quote['Security']['ISIN']
                last_close = eod_quote['LastClose']
                stock = Pwp_Stock.select().where(Pwp_Stock.isin == isin).get()
                id = stock.stock
                eod_close = eod_quote['EndOfDayPrice']
                ccy = eod_quote['Currency']
                industry = eod_quote['Security']['CategoryOrIndustry']
                f.write('%s,%s,%s,%s,%s,%s,%s,%s\n' % (market, symbol, id, last_close, eod_close, ccy, industry, isin))
            except Exception, e:
                errors.append((symbol, isin))
                print 'Error retrieving quote: %s' % eod_quote['Message']
    f.close()
    print 'Successfully retrieved prices for %s stocks.' % len(success)
    print 'Found errors retrieving prices for %s stocks.' % len(errors)
    print 'ISINs missing on %s stocks.' % len(no_isins)
csv()
