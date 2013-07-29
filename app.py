#!/usr/bin/python

from peewee import *
import urllib
import datetime
import json
from production import env
from datetime import datetime

get_closing_prices_url = 'http://www.xignite.com/xGlobalHistorical.json/GetGlobalLastClosingPrices?Identifiers={}&IdentifierType=Symbol&AdjustmentMethod=SplitOnly&_Token={}'
get_eod_quote = 'http://www.xignite.com/xGlobalHistorical.json/GetEndOfDayQuote?Identifier={}&IdentifierType=Symbol&AdjustmentMethod=SplitOnly&EndOfDayPriceMethod=LastTrade&AsOfDate={}&_Token={}'
database = MySQLDatabase(env['database']['name'], **{'passwd': env['database']['password'], 'user': env['database']['user']})

class BaseModel(Model):
    class Meta:
        database = database

class Pwp_Stocks(BaseModel):
    symbol = CharField()

    class Meta:
        db_table = 'pwp_stocks'

    def get_price_for_date(self, dt):
        url = get_eod_quote

        dt = datetime.strptime(dt, '%m/%d/%Y')
        px_history = Pwp_Stocks_History.select().join(Pwp_Stocks).where(Pwp_Stocks_History.date == dt & Pwp_Stocks.id == self.id)
        if px_history is None or px_history.count() == 0:
            print('Inserting px history to db for ({},{}).'.format(self.symbol, dt))
            formatted_url = url.format(self.symbol, dt, env['xignite']['token'])
            response = urllib.urlopen(formatted_url).read().strip().strip('"')
            px = json.loads(response)['LastClose']
            px_history = Pwp_Stocks_History(close_price=px, date=dt, stock=self)
            px_history.save()
        else:
            px_history = px_history.get()
        return px_history

class Pwp_Stocks_History(BaseModel):
    close_price = DecimalField(null=True)
    date = DateField()
    stock = ForeignKeyField(db_column='stock_id', rel_model=Pwp_Stocks)

    class Meta:
        db_table = 'pwp_stocks_history'

    def __str__(self):
        return '{} {} {}'.format(self.date, self.stock.symbol, self.close_price)

symbol = raw_input('Please enter a symbol: ')
dt = raw_input('Please enter a date (MM/DD/YYYY): ')

secs = Pwp_Stocks.select().where(Pwp_Stocks.symbol == symbol)
s = None
if secs is None or secs.count() == 0:
    s = Pwp_Stocks(symbol=symbol)
    s.save()
else:
    s = secs.get()

print(s.get_price_for_date(dt))
