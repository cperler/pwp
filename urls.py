import urllib
import json

get_last_closing_prices = 'http://www.xignite.com/xGlobalHistorical.json/GetGlobalLastClosingPrices?Identifiers=%s&IdentifierType=Symbol&AdjustmentMethod=SplitOnly&_Token=%s'
get_eod_quote_for_date = 'http://www.xignite.com/xGlobalHistorical.json/GetEndOfDayQuote?Identifier=%s&IdentifierType=Symbol&AdjustmentMethod=SplitOnly&EndOfDayPriceMethod=LastTrade&AsOfDate=%s&_T\
oken=%s'
get_last_closing_price = 'http://www.xignite.com/xGlobalHistorical.json/GetGlobalLastClosingPrice?Identifier=%s&IdentifierType=Symbol&AdjustmentMethod=SplitOnly&_Token=%s'
get_eod_quotes_for_date = 'http://www.xignite.com/xGlobalHistorical.json/GetEndOfDayQuotes?Identifiers=%s&IdentifierType=Symbol&AdjustmentMethod=SplitOnly&EndOfDayPriceMethod=LastTrade&AsOfDate=%s\
&_Token=%s'

list_exchanges = 'http://globalmaster.xignite.com/xglobalmaster.json/ListExchanges?_Token=%s'
list_sectors = 'http://globalmaster.xignite.com/xglobalmaster.json/ListSectors?_Token=%s'
list_industries = 'http://globalmaster.xignite.com/xglobalmaster.json/ListIndustries?_Token=%s'

get_instrument = 'http://globalmaster.xignite.com/xglobalmaster.json/GetInstrument?IncludeRelated=Securities&Identifier=%s&IdentifierType=Symbol&StartDate=%s&EndDate=%s&_Token=%s'
get_instruments = 'http://globalmaster.xignite.com/xglobalmaster.json/GetInstruments?IncludeRelated=Securities&Identifiers=%s&IdentifierType=Symbol&AsOfDate=%s&_Token=%s'

def request(url):
    try:
        return json.loads(urllib.urlopen(url).read().strip().strip('"'))
    except Exception, e:
        print 'Unable to retrieve from: %s' % url
        print 'Exception is %s' % e

