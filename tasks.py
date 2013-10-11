from utils import get_exchanges, get_eod_quotes, send_mail
from models import Pwp_Pwp_Stocks, ERROR, SUCCESS, SKIPPED
import datetime

def partition(lst, n):
    if n == 0:
        return [lst]
    division = len(lst) / float(n)
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in xrange(n) ]
    
def retrieve_days_prices(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    today = datetime.date.today()
    query_date = today - datetime.timedelta(days=daysback)
    filename = '%s.tsv' % str(query_date)
    f = open(filename, 'w')
    f.write('xignite market\txignite symbol\tqa id\tqa company\tqa exchange\tqa ticker\tqa currency\txignite previous_close\txignite last\txignite latest_close\txignite native currency\txignite industry sector\txignite isin\n')

    err_filename = 'err_%s.tsv' % str(today)
    e = open(err_filename, 'w')
    e.write('symbol\tisin\terror\n')
    
    success = 0
    error = 0
    skip = 0
    missing_isin = 0

    for stock in Pwp_Pwp_Stocks.select():
        if error > 5: break
        stock_id = stock.stock
        company = stock.stock_name
        exchange = stock.stock_exchange
        symbol = stock.stock_symbol
        isin = stock.isin
        local_ccy = stock.currency

        if len(isin) != 12:
            missing_isin += 1            
            msg = 'Invalid ISIN in db for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue
        
        if stock.has_db_price_for_date(query_date):
            skip += 1
            msg = 'Price already exists in db for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue
                
        quote = stock._retrieve_closing_quote_for_date()
        if quote is None:
            error += 1
            msg = 'No quote available for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
        
        security = quote.get('Security', None)
        if security is None:
            error += 1
            msg = 'No security found on xignite quote for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
        
        xignite_symbol = security['Symbol']
        xignite_market = security['Market']
        xignite_prev_close = quote['LastClose']
        xignite_last = quote['Last']
        xignite_latest_close = quote['EndOfDayPrice']
        xignite_industry = security.get('CategoryOrIndustry', '')
        xignite_ccy = quote['Currency']
        
        if xignite_market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 
                              'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
            error += 1
            msg = 'Exchange %s not supported for (%s, %s).' % (xignite_market, symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
            
        if xignite_last is None or xignite_prev_close is None or xignite_latest_close is None:
            error += 1
            msg = 'Unable to find price for (%s, %s).' % (symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue            
        
        if local_ccy != xignite_ccy:
            error += 1
            msg = 'Persisted currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, xignite_ccy, symbol, isin)
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
            print msg
            continue
        
        print 'Px %s found on xignite quote for (%s, %s).' % (xignite_latest_close, symbol, isin)
        if persist:
            try:
                stock.update_closing_price_for_date(xignite_latest_close)
            except Exception as ex:
                error += 1
                msg = 'Exception %s persisting px %s for (%s, %s).' % (ex, xignite_latest_close, symbol, isin)
                e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
                print msg
                continue
            
        try:
            f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % 
                    (xignite_market.encode('utf-8'), 
                     xignite_symbol, 
                     stock_id, 
                     company, 
                     exchange, 
                     symbol, 
                     local_ccy, 
                     xignite_prev_close, 
                     xignite_last, 
                     xignite_latest_close, 
                     xignite_ccy.encode('utf-8'), 
                     xignite_industry.encode('utf-8'), 
                     isin.encode('utf-8')))
            success += 1
        except Exception as ex:            
            print 'Exception writing to file for (%s, %s).' % (symbol, isin)
    f.close()
    e.close()
    
    print 'Successfully updated prices for %s stocks.' % success
    print 'Skipped updating %s stocks as they were already current.' % skip
    print 'Found errors updating %s stocks.' % error
    print 'ISINs missing on %s stocks.' % missing_isin

    send_mail('craig.perler@gmail.com', emailto, '[QA] xIgnite Report: %s' % str(today), '<h3>Please find the latest pricing data from xIgnite attached.</h3>', files=[filename, err_filename])
    
def batch_retrieve_days_prices(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    isins = [stock.identifier_and_suffix()[0] for stock in Pwp_Pwp_Stocks.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.stock_symbol) for stock in Pwp_Pwp_Stocks.select() if stock.isin is None]
    
    print 'Updating %s stocks that have ISINs.' % len(isins)
    print 'Not updating %s stocks that do not have ISINs.' % len(no_isins)

    all_isins = []
    success = []
    skips = []
    errors = []

    today = datetime.date.today()
    query_date = today - datetime.timedelta(days=daysback)
    filename = '%s.tsv' % str(query_date)
    f = open(filename, 'w')
    f.write('xignite market\txignite symbol\tqa id\tqa company\tqa exchange\tqa ticker\tqa currency\txignite previous_close\txignite last\txignite latest_close\txignite native currency\txignite industry sector\txignite isin\n')
    
    for lst in partition(isins, len(isins) / 100):      
        eod_quotes = get_eod_quotes(lst, 'ISIN', query_date)
        for eod_quote in eod_quotes:
            if eod_quote:
                security = eod_quote.get('Security', None)
                if security:
                    xignite_market = security['Market']
                    xignite_symbol = security['Symbol']
                    xignite_isin = security['ISIN']
                    xignite_prev_close = eod_quote['LastClose']
                    xignite_last = eod_quote['Last']
                    xignite_latest_close = eod_quote['EndOfDayPrice']
                    
                    if xignite_last is None or xignite_last == 0.0 or xignite_prev_close is None or xignite_prev_close == 0.0 or xignite_latest_close is None or xignite_latest_close == 0.0:
                        errors.append((xignite_symbol, xignite_isin, 'No or 0.0 value price available for (%s, %s).' % (xignite_symbol, xignite_isin)))
                        all_isins.append(xignite_isin)
                    
                    try:                    
                        stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == xignite_isin).get()
                    except:
                        all_isins.append(xignite_isin)
                        errors.append((xignite_symbol, xignite_isin, 'Unable to locate stock for (%s, %s).' % (xignite_symbol, xignite_isin)))
                        continue

                    stock_id = stock.stock
                    company = stock.stock_name
                    exchange = stock.stock_exchange
                    symbol = stock.stock_symbol
                    isin = stock.isin

                    if xignite_market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 
                                          'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
                        all_isins.append(isin)
                        errors.append((symbol, isin, 'Exchange %s not supported for (%s, %s).' % (xignite_market, symbol, isin)))

                    local_ccy = stock.currency
                    xignite_ccy = eod_quote['Currency']
                    if local_ccy != xignite_ccy:
                        all_isins.append(isin)
                        errors.append((symbol, isin, 'Persisted currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, xignite_ccy, symbol, isin)))
                    
                    xignite_industry = security.get('CategoryOrIndustry', '')
                    
                    try:
                        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % 
                                (xignite_market.encode('utf-8'), 
                                 symbol, 
                                 stock_id, 
                                 company, 
                                 exchange, 
                                 symbol, 
                                 local_ccy, 
                                 xignite_prev_close, 
                                 xignite_last, 
                                 xignite_latest_close, 
                                 xignite_ccy.encode('utf-8'), 
                                 xignite_industry.encode('utf-8'), 
                                 isin.encode('utf-8')))
                    except:
                        all_isins.append(isin)
                        errors.append((symbol, isin, 'Error writing data to file for (%s, %s).' % (symbol, isin)))
                        continue
                    
                    if persist:
                        stock.update_closing_price_for_date(px=xignite_prev_close)
                    all_isins.append(isin)
                else:
                    print 'Error retrieving quote: %s' % eod_quote['Message']
            else:
                print 'Unexpected error occurred.'
    '''
    for isin in isins:
        if isin not in all_isins:
            try:
                stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == isin).get()
                symbol = stock.stock_symbol
                print 'Unable to retrieve quote for (%s, %s)' % (symbol, isin)
                errors.append((symbol, isin, 'Unable to retrieve quote for (%s, %s)' % (symbol, isin)))
            except:
                print 'Unable to retrieve quote for (%s, %s)' % ('Unknown Symbol', isin)
                errors.append((symbol, isin, 'Unable to retrieve quote for (%s, %s)' % ('Unknown Symbol', isin)))
    '''         
    f.close()

    err_filename = 'err_%s.tsv' % str(today)
    e = open(err_filename, 'w')
    e.write('symbol\tisin\terror\n')
    for error_details in errors:
        symbol = error_details[0]
        isin = error_details[1]
        msg = error_details[2]
        if symbol or isin:
            e.write('%s\t%s\t%s\n' % (symbol, isin, msg))
    e.close()

    print 'Successfully updated prices for %s stocks.' % len(success)
    print 'Skipped updating %s stocks as they were already current.' % len(skips)
    print 'Found errors updating %s stocks.' % len(errors)
    print 'ISINs missing on %s stocks.' % len(no_isins)

    send_mail('craig.perler@gmail.com', emailto, '[QA] xIgnite Report: %s' % str(today), '<h3>Please find the latest pricing data from xIgnite attached.</h3>', files=[filename, err_filename], server='smtp.gmail.com', username='craig.perler@gmail.com', password='')    