from peewee import *
from passwords import env
from urls import *
import datetime

xignite_token = env['xignite']['token']
database = MySQLDatabase(env['db']['name'], **{'passwd': env['db']['password'], 'host': env['db']['host'], 'user': env['db']['user']})

SKIPPED = 0
ERROR = 1
SUCCESS = 2

class UnknownFieldType(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database

class Pwp_Pwp_Participant_Picks(BaseModel):
    last_close = DecimalField()
    long_short = IntegerField()
    notes = CharField()
    pick = IntegerField(primary_key=True)
    start_price = DecimalField()
    stock = IntegerField(db_column='stock_id')
    trade_date = IntegerField()
    trade_price = DecimalField()
    traded = IntegerField()
    uid = IntegerField()
    updated = IntegerField()
    year = IntegerField()

    class Meta:
        db_table = 'pwp_pwp_participant_picks'

class Pwp_Pwp_Participant_Rank(BaseModel):
    # FIXME: "class" is a reserved word
    #class = CharField()
    rank = IntegerField()
    uid = IntegerField()
    year = IntegerField()
    ytd_perf = DecimalField()

    class Meta:
        db_table = 'pwp_pwp_participant_rank'

class Pwp_Pwp_Participants(BaseModel):
    active = IntegerField()
    charity = IntegerField(db_column='charity_id')
    charity_reason = CharField()
    # FIXME: "class" is a reserved word
    #class = CharField()
    private_email = IntegerField()
    private_portfolio = IntegerField()
    trades = IntegerField()
    trades_used = IntegerField()
    uid = IntegerField()
    year = IntegerField()

    class Meta:
        db_table = 'pwp_pwp_participants'

class Pwp_Exchange_Map(BaseModel):
    capiq = CharField(null=True, primary_key=True)
    xignite = CharField(null=True)

    class Meta:
        db_table = 'pwp_exchange_map'

class Pwp_Pwp_Stocks(BaseModel):
    approved = IntegerField()
    change_amt = DecimalField()
    change_pct = DecimalField()
    currency = CharField()
    dividend_yield = DecimalField()
    isin = CharField()
    last_close = DecimalField()
    market_cap = DecimalField()
    prev_close = DecimalField()
    stock_exchange = CharField()
    stock = IntegerField(db_column='stock_id', primary_key=True)
    stock_name = CharField()
    stock_symbol = CharField()
    tags = CharField()
    updated = CharField()
    ytd_high = DecimalField()
    ytd_low = DecimalField()
    ytd_total_change = DecimalField()

    class Meta:
        db_table = 'pwp_pwp_stocks'

    def get_xignite_exchange(self):
        exchange_list = Pwp_Exchange_Map.select().where(Pwp_Exchange_Map.capiq == self.stock_exchange)
        if exchange_list.count() == 0:
            return None
        elif exchange_list.count() == 1:
            return exchange_list.get().xignite
        raise Exception('Too many exchange codes available in mapping table for %s.' % self.stock_exchange)

    def identifier_and_suffix(self, identifier_type='ISIN'):        
        identifier = self.isin
        if identifier is None or identifier_type == 'Symbol':
            identifier_type = 'Symbol'
            identifier = self.stock_symbol
            
        exchange = self.get_xignite_exchange()
        identifier_with_exchange = identifier

        if exchange:
            identifier_with_exchange = '%s.%s' % (identifier, exchange)        
        return (identifier_with_exchange, identifier_type)    

    def _retrieve_last_closing_price(self, identifier_type='ISIN'):
        identifier_with_exchange, identifier_type = self.identifier_and_suffix(identifier_type)        
        response = request(get_last_closing_price % (identifier_with_exchange, identifier_type, xignite_token))
        return response.get('LastClose', 0.0)

    def _retrieve_closing_price_for_date(self, dt=datetime.date.today(), identifier_type='ISIN'):
        return self._retrieve_closing_quote_for_date(dt, identifier_type).get('ExchangeClose', 0.0)
    
    def _retrieve_closing_quote_for_date(self, dt=datetime.date.today(), identifier_type='ISIN'):
        identifier_with_exchange, identifier_type = self.identifier_and_suffix(identifier_type)        
        response = request(get_eod_quote_for_date % (identifier_with_exchange, identifier_type, dt, xignite_token))
        return response

    def _get_history_from_db(self, dt=datetime.date.today()):
        eod_quote_for_date = Pwp_Pwp_Xignite_Stocks_History.select().where(
                               (Pwp_Pwp_Xignite_Stocks_History.stock==self.stock) & 
                               (Pwp_Pwp_Xignite_Stocks_History.date==dt))
        if eod_quote_for_date.count() == 1:
            return eod_quote_for_date.get()
        return None
    
    def _get_closing_price_from_db(self, dt=datetime.date.today()):
        historical_record = self._get_history_from_db(dt)
        return historical_record.last_close if historical_record else None

    def has_db_price_for_date(self, dt):
        return self._get_closing_price_from_db(dt) is not None
    
    def closing_price_for_date(self, dt=datetime.date.today()):
        if self.has_db_price_for_date(dt):
            return self._get_closing_price_from_db(dt)
        return self._retrieve_closing_price_for_date(dt)

    def update_closing_price_for_date(self, px=None, dt=datetime.date.today(), identifier_type='ISIN'):
        if self.has_db_price_for_date(dt):
            print 'Record already exists for (%s, %s) -- not updating.' % (self.stock_symbol, dt)
            return SKIPPED
        else:
            px = px if px else self._retrieve_closing_price_for_date(dt, identifier_type)
            if px is None or px == 0.0:
                print 'Unable to retrieve px for (%s, %s) -- not persisting.' % (self.stock_symbol, dt)
                return ERROR
            else:
                Pwp_Pwp_Xignite_Stocks_History.create(last_close=px, date=dt, stock=self.stock)
                print 'Persisted px for (%s, %s).' % (self.stock_symbol, dt)
                return SUCCESS



class Pwp_Pwp_Xignite_Stocks_History(BaseModel):
    date = CharField()
    last_close = FloatField()
    stock = IntegerField(db_column='stock_id', primary_key=True)

    class Meta:
        db_table = 'pwp_pwp_xignite_stocks_history'


class Pwp_Pwp_Stocks_History(BaseModel):
    date = CharField()
    last_close = FloatField()
    stock = IntegerField(db_column='stock_id', primary_key=True)

    class Meta:
        db_table = 'pwp_pwp_stocks_history'

class Pwp_Pwp_Stocks_Year(BaseModel):
    currency = CharField()
    dividend_yield = DecimalField()
    end_market_cap = DecimalField()
    isin = CharField()
    last_close = DecimalField()
    prev_close = DecimalField()
    start_market_cap = DecimalField()
    start_price = DecimalField()
    stock_exchange = CharField()
    stock = IntegerField(db_column='stock_id')
    stock_symbol = CharField()
    updated = IntegerField()
    year = IntegerField()
    ytd_high = DecimalField()
    ytd_low = DecimalField()
    ytd_total_change = DecimalField()

    class Meta:
        db_table = 'pwp_pwp_stocks_year'

class Pwp_Pwp_Xignite_Stocks(BaseModel):
    active = IntegerField()
    change_amt = DecimalField()
    change_pct = DecimalField()
    currency = CharField()
    delist = IntegerField()
    dividend_yield = DecimalField()
    isin = CharField()
    last_close = DecimalField()
    market_cap = DecimalField()
    prev_close = DecimalField()
    stock_exchange = CharField()
    stock = IntegerField(db_column='stock_id', primary_key=True)
    stock_name = CharField()
    stock_symbol = CharField()
    updated = CharField()
    ytd_change = DecimalField()
    ytd_high = DecimalField()
    ytd_low = DecimalField()
    
    class Meta:
        db_table = 'pwp_pwp_xignite_stocks'
        
    def get_xignite_exchange(self):
        exchange_list = Pwp_Exchange_Map.select().where(Pwp_Exchange_Map.capiq == self.stock_exchange)
        if exchange_list.count() == 0:
            return None
        elif exchange_list.count() == 1:
            return exchange_list.get().xignite
        raise Exception('Too many exchange codes available in mapping table for %s.' % self.stock_exchange)

    def identifier_and_suffix(self, identifier_type='ISIN'):        
        identifier = self.isin
        if identifier is None or identifier_type == 'Symbol':
            identifier_type = 'Symbol'
            identifier = self.stock_symbol
            
        exchange = self.get_xignite_exchange()
        identifier_with_exchange = identifier

        if exchange:
            identifier_with_exchange = '%s.%s' % (identifier, exchange)        
        return (identifier_with_exchange, identifier_type)    

    def _retrieve_last_closing_price(self, identifier_type='ISIN'):
        identifier_with_exchange, identifier_type = self.identifier_and_suffix(identifier_type)        
        response = request(get_last_closing_price % (identifier_with_exchange, identifier_type, xignite_token))
        return response.get('LastClose', 0.0)

    def _retrieve_closing_price_for_date(self, dt=datetime.date.today(), identifier_type='ISIN'):
        return self._retrieve_closing_quote_for_date(dt, identifier_type).get('ExchangeClose', 0.0)
    
    def _retrieve_closing_quote_for_date(self, dt=datetime.date.today(), identifier_type='ISIN'):
        identifier_with_exchange, identifier_type = self.identifier_and_suffix(identifier_type)        
        response = request(get_eod_quote_for_date % (identifier_with_exchange, identifier_type, dt, xignite_token))
        return response

    def _get_history_from_db(self, dt=datetime.date.today()):
        eod_quote_for_date = Pwp_Pwp_Xignite_Stocks_History.select().where(
                               (Pwp_Pwp_Xignite_Stocks_History.stock==self.stock) & 
                               (Pwp_Pwp_Xignite_Stocks_History.date==dt))
        if eod_quote_for_date.count() == 1:
            return eod_quote_for_date.get()
        return None
    
    def _get_closing_price_from_db(self, dt=datetime.date.today()):
        historical_record = self._get_history_from_db(dt)
        return historical_record.last_close if historical_record else None

    def has_db_price_for_date(self, dt):
        return self._get_closing_price_from_db(dt) is not None
    
    def closing_price_for_date(self, dt=datetime.date.today()):
        if self.has_db_price_for_date(dt):
            return self._get_closing_price_from_db(dt)
        return self._retrieve_closing_price_for_date(dt)
    
    def update_data(self, prev_close, last_close, change_amt, change_pct, market_cap, div_yield):
        self.prev_close = prev_close
        self.last_close = last_close
        self.change_amt = change_amt
        self.change_pct = change_pct
        self.market_cap = market_cap
        self.dividend_yield = div_yield
        self.save()

    def update_closing_price_for_date(self, px=None, dt=datetime.date.today(), identifier_type='ISIN'):
        if self.has_db_price_for_date(dt):
            print 'Record already exists for (%s, %s) -- not updating.' % (self.stock_symbol, dt)
            return SKIPPED
        else:
            px = px if px else self._retrieve_closing_price_for_date(dt, identifier_type)
            if px is None or px == 0.0:
                print 'Unable to retrieve px for (%s, %s) -- not persisting.' % (self.stock_symbol, dt)
                return ERROR
            else:
                Pwp_Pwp_Xignite_Stocks_History.create(last_close=px, date=dt, stock=self.stock)
                print 'Persisted px for (%s, %s).' % (self.stock_symbol, dt)
                return SUCCESS

def stock(symbol):
    return Pwp_Pwp_Xignite_Stocks.select().where(Pwp_Pwp_Xignite_Stocks.stock_symbol==symbol)[0]
