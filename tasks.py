from utils import get_exchanges, get_eod_quotes, send_mail
from models import Pwp_Pwp_Stocks, ERROR, SUCCESS, SKIPPED
import datetime

def get_exchange_list():
    return [e['ProviderValue'] for e in get_exchanges()['Values']]

def partition(lst, n):
    if n == 0:
        return [lst]
    division = len(lst) / float(n)
    return [ lst[int(round(division * i)): int(round(division * (i + 1)))] for i in xrange(n) ]
    
def retrieve_days_prices():
    for stock in Pwp_Pwp_Stocks.select():
        stock.update_closing_price_for_date()
    
def batch_retrieve_days_prices(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    isins = [stock.identifier_and_suffix()[0] for stock in Pwp_Pwp_Stocks.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.stock_symbol) for stock in Pwp_Pwp_Stocks.select() if stock.isin is None]
    
    print 'Updating {} stocks that have ISINs.'.format(len(isins))
    print 'Not updating {} stocks that do not have ISINs.'.format(len(no_isins))

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
                    xignite_last = eod_quote['Last']
                    xignite_last_close = eod_quote['ExchangeClose']
                    xignite_eod_price = eod_quote['EndOfDayPrice']
                    
                    if xignite_last is None or xignite_last == 0.0 or xignite_last_close is None or xignite_last_close == 0.0 or xignite_eod_price is None or xignite_eod_price == 0.0:
                        errors.append((xignite_symbol, xignite_isin, 'No or 0.0 value price available for (%s, %s).' % (xignite_symbol, xignite_isin)))
                        continue
                    
                    try:                    
                        stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == xignite_isin).get()
                    except:
                        errors.append((xignite_symbol, xignite_isin, 'Unable to locate stock for (%s, %s).' % (xignite_symbol, xignite_isin)))
                        continue

                    stock_id = stock.stock
                    company = stock.stock_name
                    exchange = stock.stock_exchange
                    symbol = stock.stock_symbol
                    isin = stock.isin

                    if xignite_market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 
                                          'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
                        errors.append((symbol, isin, 'Exchange %s not supported for (%s, %s).' % (xignite_market, symbol, isin)))
                        continue

                    local_ccy = stock.currency
                    xignite_ccy = eod_quote['Currency']
                    if local_ccy != xignite_ccy:
                        errors.append((symbol, isin, 'Persisted currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, xignite_ccy, symbol, isin)))
                        continue
                    
                    xignite_industry = security.get('CategoryOrIndustry', '')
                    f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % 
                            (xignite_market, symbol, stock_id, company, exchange, symbol, local_ccy, 
                             xignite_last_close, xignite_last, xignite_eod_price, xignite_ccy, xignite_industry, isin))
                    
                    if persist:
                        status = stock.update_closing_price_for_date(px=xignite_last_close)
                        
                        if status == ERROR:                        
                            errors.append((symbol, isin))
                        elif status == SUCCESS:
                            success.append((symbol, isin))
                        elif status == SKIPPED:
                            skips.append((symbol, isin))
                    all_isins.append(isin)
                else:
                    print 'Error retrieving quote: %s' % eod_quote['Message']
            else:
                print 'Unexpected error occurred.'

    for isin in isins:
        if isin not in all_isins:
            stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == isin).get()
            symbol = stock.stock_symbol            
            print 'Unable to retrieve quote for (%s, %s)' % (symbol, isin)
            errors.append((symbol, isin))

    print 'Successfully updated prices for %s stocks.' % len(success)
    print 'Skipped updating %s stocks as they were already current.' % len(skips)
    print 'Found errors updating %s stocks.' % len(errors)
    print 'ISINs missing on %s stocks.' % len(no_isins)
    

def batch_retrieve_and_email(persist=True, daysback=0, emailto=['craig.perler@gmail.com']):
    isins = [stock.identifier_and_suffix()[0] for stock in Pwp_Pwp_Stocks.select() if stock.isin is not None]
    no_isins = [(stock.stock, stock.ticker) for stock in Pwp_Pwp_Stocks.select() if stock.isin is None]
    
    print 'Updating {} stocks that have ISINs.'.format(len(isins))
    print 'Not updating {} stocks that do not have ISINs.'.format(len(no_isins))

    all_isins = []
    success = []
    skips = []
    errors = []
    
    today = datetime.date.today() - datetime.timedelta(days=daysback)
    filename = '%s.tsv' % str(today)
    f = open(filename, 'w')
    f.write('xignite market\txignite symbol\tqa id\tqa company\tqa exchange\tqa ticker\tqa currency\txignite previous_close\txignite last\txignite latest_close\txignite native currency\txignite industry sector\txignite isin\n')
    for lst in partition(isins, len(isins) / 100):
        eod_quotes = get_eod_quotes(lst, 'ISIN', today)
        if not eod_quotes:
            continue
        for eod_quote in eod_quotes:
            try:
                if eod_quote:
                    security = eod_quote.get('Security', None)
                    if security:
                        market = security['Market']
                        symbol = security['Symbol']
                        isin = eod_quote['Security']['ISIN']
                        last = eod_quote['Last']
                        last_close = eod_quote['ExchangeClose']
                        stock = Pwp_Pwp_Stocks.select().where(Pwp_Pwp_Stocks.isin == isin).get()
                        id = stock.stock
                        company = stock.company_name
                        exchange = stock.exchange
                        ticker = stock.ticker
                        
                        if market in ['MILAN', 'NAIROBI', 'MEXICO', 'MANILA', 'HOCHIMINH STOCK EXCHANGE', 'KARACHI', 'JAKARTA', 'BOGOTA', 'CAIRO']:
                            raise Exception('Exchange %s not supported for (%s, %s).' % (market, ticker, isin))
                        
                        ticker = stock.ticker
                        local_ccy = stock.currency
                        eod_close = eod_quote['EndOfDayPrice']
                        ccy = eod_quote['Currency']
                        if local_ccy != ccy:
                            errors.append((ticker, isin, 'PWP currency %s does not match xIgnite currency %s for (%s, %s).' % (local_ccy, ccy, ticker, isin)))
                        industry = eod_quote['Security']['CategoryOrIndustry']
                        f.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (market, symbol, id, company, exchange, ticker, local_ccy, last_close, last, eod_close, ccy, industry, isin))
        
                        if persist:
                            status = stock.update_closing_price_for_date(px=last_close)
                            if status == ERROR:                        
                                errors.append((symbol, isin))
                            elif status == SUCCESS:
                                success.append((symbol, isin))
                            elif status == SKIPPED:
                                skips.append((symbol, isin))
                            all_isins.append(isin)
                    else:
                        print eod_quote

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