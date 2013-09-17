from models import *
from utils import *
import datetime
from datetime import timedelta

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

def batch_retrieve_and_email(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    isins = [stock.identifier_and_suffix() for stock in Pwp_Stock.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.ticker) for stock in Pwp_Stock.select() if stock.isin is None]
    print 'Updating %s stocks that have isins.' % len(isins)

    success = []
    errors = []
    
    today = datetime.date.today() - timedelta(days=daysback)
    filename = '%s.tsv' % str(today)
    f = open(filename, 'w')
    f.write('xignite market\txignite symbol\tqa id\tqa company\tqa exchange\tqa ticker\tqa currency\txignite previous_close\txignite last\txignite latest_close\txignite native currency\txignite industry sector\txignite isin\n')
    for lst in partition(isins, len(isins) / 50):
        eod_quotes = get_eod_quotes(lst, 'ISIN', today)	# api call to xignite
        if not eod_quotes:
            continue
        for eod_quote in eod_quotes:
            try:
                market = eod_quote['Security']['Market']
                symbol = eod_quote['Security']['Symbol']
                isin = eod_quote['Security']['ISIN']
                last = eod_quote['Last']
                last_close = eod_quote['LastClose']
                stock = Pwp_Stock.select().where(Pwp_Stock.isin == isin).get()
                id = stock.stock
                company = stock.company_name
                exchange = stock.exchange
                ticker = stock.ticker
                
                # xignite doesn't have data for certain markets:
                if market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
                    raise Exception('Exchange %s not supported for (%s, %s).' % (market, ticker, isin))
                
                ticker = stock.ticker
                local_ccy = stock.currency
                eod_close = eod_quote['EndOfDayPrice']
                ccy = eod_quote['Currency']
				
                # xignite has a different currency than the db for some names:
                if local_ccy != ccy:
                    errors.append((ticker, isin, 'PWP currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, ccy, ticker, isin)))
                industry = eod_quote['Security']['CategoryOrIndustry']
				
                f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (market, symbol, id, company, exchange, ticker, local_ccy, last_close, last, eod_close, ccy, industry, isin))

				if persist:
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
                symbol_from_err = 'Unknown Symbol'
                isin_from_err = 'Unknown ISIN'
                
                error_msg = eod_quote.get('Message', None)
                if eod_quote:
                    sec_from_err = eod_quote.get('Security', None)
                    if sec_from_err:
                        symbol_from_err = sec_from_err.get('Symbol', 'Unknown Symbol')
                        isin_from_err = sec_from_err.get('ISIN', 'Unknown ISIN')
                        error_msg = 'Unable to retrieve data for (%s, %s).' % (symbol_from_err, isin_from_err)
                    
                        if sec_from_err.get('Market') in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
                            error_msg = str(e)
                    else:
                        isin_from_err = error_msg.replace('No match found for this ISIN (', '').replace(').', '')

                errors.append((symbol_from_err, isin_from_err, error_msg))
                
    f.close()

    err_filename = 'err_%s.tsv' % str(today)
    e = open(err_filename, 'w')
    e.write('symbol\tisin\terror\n')
    for error_details in errors:
        symbol = error_details[0]
        isin = error_details[1]
        msg = error_details[2]
        e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
    e.close()
    
    print 'Successfully retrieved prices for %s stocks.' % len(success)
    print 'Found errors retrieving prices for %s stocks.' % len(errors)
    print 'ISINs missing on %s stocks.' % len(no_isins)
    #print '\n'.join(errors)

    send_mail('craig.perler@gmail.com', emailto, '[QA] xIgnite Report: %s' % str(today), '<h3>Please find the latest pricing data from xIgnite attached.</h3>', files=[filename, err_filename], server='smtp.gmail.com', username='craig.perler@gmail.com', password='p0rsche9')
