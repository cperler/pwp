from passwords import env
import urls
from urls import *

xignite_token = env['xignite']['token']

def get_exchanges():
    exchanges = request(list_exchanges % xignite_token)
    return [e['ProviderValue'] for e in exchanges['Values']]

def get_sectors():
    return request(list_sectors % xignite_token)

def get_industries():
    return request(list_industries % xignite_token)

def get_instrument_details(identifier, identifier_type, start, end):
    return request(get_instrument % (identifier, identifier_type, start, end, xignite_token))

def get_instruments_details(identifier_list, identifier_type, as_of):
    return request(get_instruments % (','.join(identifier_list), identifier_type, as_of, xignite_token))

def get_eod_quotes(identifier_list, identifier_type, as_of):
    return request(get_eod_quotes_for_date % (','.join(identifier_list), identifier_type, as_of, xignite_token))
                        
def get_last_closing_prices(identifier_list, identifier_type):
    return request(urls.get_last_closing_prices % (','.join(identifier_list), identifier_type, xignite_token))
	
def get_symbols_by_exchange(exchange_code, as_of_date):
	return request(urls.get_symbols_by_exchange % (exchange_code, as_of_date, xignite_token))

import smtplib, os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
from email import encoders

def send_mail( send_from, send_to, subject, text, files=[], server="localhost", port=587, username='', password='', isTls=True):
    msg = MIMEMultipart('alternative')
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text, 'html') )
    
    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="{0}"'.format(os.path.basename(f)))
        msg.attach(part)
        
    smtp = smtplib.SMTP(server) #, port)
    smtp.ehlo()
    if isTls: smtp.starttls()
    smtp.ehlo()
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
