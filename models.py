from peewee import *
from passwords import env
from urls import *
import datetime

xignite_token = env['xignite']['token']
database = MySQLDatabase(env['db']['name'], **{'passwd': env['db']['password'], 'host': env['db']['host'], 'user': env['db']['user']})

class UnknownFieldType(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database

class Countries(BaseModel):
    country_alpha2 = CharField()
    country_alpha3 = CharField()
    country = IntegerField(db_column='country_id')
    country_name = CharField()
    country_numeric = IntegerField()

    class Meta:
        db_table = 'countries'

class Pwp_Cache(BaseModel):
    data_key = CharField()
    data_value = TextField(null=True)
    internal_key = DateTimeField()

    class Meta:
        db_table = 'pwp_cache'

class Pwp_Charity(BaseModel):
    approved = IntegerField()
    charity = IntegerField(db_column='charity_id')
    charity_name = CharField()
    create_by = IntegerField()
    create_date = DateTimeField()
    email = CharField()
    last_update_by = IntegerField()
    last_update_date = DateTimeField()
    website = CharField()

    class Meta:
        db_table = 'pwp_charity'

class Pwp_Contest(BaseModel):
    setting = CharField()
    setting = IntegerField(db_column='setting_id')
    value = CharField(null=True)

    class Meta:
        db_table = 'pwp_contest'

class Pwp_Contest_Year(BaseModel):
    master_winnings = IntegerField()
    novice_winnings = IntegerField()
    professional_winnings = IntegerField()
    year = IntegerField()

    class Meta:
        db_table = 'pwp_contest_year'

class Pwp_Donation(BaseModel):
    amount = CharField()
    cardtype = CharField(db_column='cardType')
    cardholdername = CharField(db_column='cardholderName')
    countryname = CharField(db_column='countryName')
    donation = IntegerField(db_column='donation_id')
    email = CharField()
    locality = CharField()
    payment_date = CharField()
    payment_status = UnknownFieldType()
    phone = CharField()
    postalcode = CharField(db_column='postalCode')
    region = CharField()
    streetaddress = CharField(db_column='streetAddress')

    class Meta:
        db_table = 'pwp_donation'

class Pwp_Pages(BaseModel):
    content = TextField(null=True)
    description = CharField(null=True)
    keywords = CharField(null=True)
    page = IntegerField(db_column='page_id')
    page_name = CharField()
    rich_editing = CharField(null=True)
    title = CharField(null=True)

    class Meta:
        db_table = 'pwp_pages'

class Pwp_Participant(BaseModel):
    about_pwp = TextField()
    active = IntegerField()
    age = IntegerField(null=True)
    bio = TextField(null=True)
    # FIXME: "class" is a reserved word
    #class = CharField(null=True)
    comment_pwp = TextField()
    company_address = CharField(null=True)
    company_address_2 = CharField(null=True)
    company_city = CharField(null=True)
    company_name = CharField(null=True)
    company_state = IntegerField(null=True, db_column='company_state_id')
    company_zip_code = CharField(null=True)
    country = CharField(null=True)
    create_date = DateTimeField(null=True)
    email = CharField()
    email_confirm_key = CharField()
    email_confirmed = IntegerField()
    email_private = UnknownFieldType()
    first_name = CharField(null=True)
    first_stock_year = IntegerField(null=True)
    investing_exp = IntegerField(null=True)
    isadmin = IntegerField(db_column='isAdmin')
    last_name = CharField(null=True)
    last_update_date = DateTimeField(null=True)
    password = CharField()
    personal_address = CharField(null=True)
    personal_address_2 = CharField(null=True)
    personal_city = CharField(null=True)
    personal_state_code = CharField(null=True)
    personal_zip_code = CharField(null=True)
    phone = CharField()
    picture = TextField(null=True)
    portfolio_private = UnknownFieldType()
    pw_reset_key = CharField(null=True)
    user = BigIntegerField(db_column='user_id')
    username = CharField()

    class Meta:
        db_table = 'pwp_participant'

class Pwp_Participant_Year(BaseModel):
    accepted_terms = IntegerField()
    charity = IntegerField(null=True, db_column='charity_id')
    charity_reason = CharField(null=True)
    # FIXME: "class" is a reserved word
    #class = CharField()
    class_standing = IntegerField(null=True)
    contest_status = CharField()
    daily_perf = FloatField(null=True)
    experience = TextField()
    participant = IntegerField(db_column='participant_id')
    payment_date = DateTimeField(null=True)
    payment_status = UnknownFieldType()
    year = UnknownFieldType()
    ytd_perf = FloatField(null=True)

    class Meta:
        db_table = 'pwp_participant_year'

class Pwp_Participant_Year_Stock(BaseModel):
    comments = CharField()
    participant = IntegerField(db_column='participant_id')
    position = CharField()
    sequence = IntegerField()
    stock = IntegerField(db_column='stock_id')
    year = UnknownFieldType()

    class Meta:
        db_table = 'pwp_participant_year_stock'

class Pwp_Settings(BaseModel):
    last_stock_update = DateTimeField()

    class Meta:
        db_table = 'pwp_settings'

class Pwp_Stock(BaseModel):
    active = IntegerField(null=True)
    company_name = CharField()
    create_date = DateTimeField()
    currency = CharField(null=True)
    exchange = CharField()
    isin = CharField(null=True)
    last_update_date = DateTimeField()
    notes = CharField(null=True)
    orig_ticker_exchange = CharField(null=True)
    stock = IntegerField(db_column='stock_id', primary_key=True)
    ticker = CharField()

    class Meta:
        db_table = 'pwp_stock'

    def get_last_closing_price(self, identifier_type='ISIN'):
        identifier = self.isin
        if identifier is None:
            identifier_type = 'Symbol'
            identifier = self.ticker
            
        response = request(get_last_closing_price % (identifier, identifier_type, xignite_token))
        return response.get('LastClose', 0.0)

    def get_eod_quote_for_date(self, dt=datetime.date.today(), identifier_type='ISIN'):
        eod_quote_for_date = Pwp_Stock_Price_History.select().where((Pwp_Stock_Price_History.stock==self.stock) & (Pwp_Stock_Price_History.date==dt))
        last_close_for_date = 0.0

        history_record = None
        if eod_quote_for_date.count() == 0:
            identifier = self.isin
            if identifier is None:
                identifier_type = 'Symbol'
                identifier = self.ticker
            
            response = request(get_eod_quote_for_date % (identifier, identifier_type, dt, xignite_token))
            last_close_for_date = response.get('LastClose', 0.0)
        else:
            history_record = eod_quote_for_date.get()
            last_close_for_date = history_record.close
        return (last_close_for_date, history_record)             

    def update_eod_quote_for_date(self, dt=datetime.date.today(), identifier_type='ISIN'):
        last_close_for_date, history_record = self.get_eod_quote_for_date(dt, identifier_type)

        if history_record:
            print 'Record already exists for (%s, %s) -- not updating.' % (self.ticker, dt)
        else:
            if last_close_for_date == 0.0:
                print 'Unable to retrieve px for (%s, %s) -- not persisting.' % (self.ticker, dt)
            else:
                Pwp_Stock_Price_History.create(close=last_close_for_date, date=dt, stock=self.stock)
                print 'Persisted px for (%s, %s).' % (self.ticker, dt)

class Pwp_Stock_Price_History(BaseModel):
    close = FloatField()
    date = DateField(primary_key=True)
    stock = IntegerField(db_column='stock_id', primary_key=True)

    class Meta:
        db_table = 'pwp_stock_price_history'

class Pwp_Stock_Year(BaseModel):
    begin_close = FloatField()
    begin_close_adj = FloatField()
    begin_market_cap_usd = FloatField()
    dividend_yield = FloatField()
    last_update_date = DateTimeField()
    latest_close = FloatField()
    latest_close_adj = FloatField()
    market_cap_usd = FloatField()
    previous_close = FloatField()
    stock = IntegerField(db_column='stock_id')
    year = UnknownFieldType()
    ytd_high = FloatField()
    ytd_low = FloatField()
    ytd_total_return = FloatField()

    class Meta:
        db_table = 'pwp_stock_year'

class Pwp_Users(BaseModel):
    email = CharField()
    password = CharField()
    pw_reset_key = CharField(null=True)
    user = IntegerField(db_column='user_id')
    username = CharField()

    class Meta:
        db_table = 'pwp_users'

class Us_States(BaseModel):
    abbrev = CharField()
    name = CharField()

    class Meta:
        db_table = 'us_states'
