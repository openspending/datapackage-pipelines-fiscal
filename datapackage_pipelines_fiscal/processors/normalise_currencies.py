import os
import datetime
import logging

import requests
from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal, ROUND_DOWN

params, datapackage, res_iter = ingest()

ACCESS_KEY = os.environ.get(
    'CURRENCYLAYER_API_KEY',
    'e97f996f7ae32e065a2061a4142824ca'
)
CURRENCY_API = 'http://apilayer.net/api/historical?access_key={key}&' + \
               'date={date:%Y-%m-%d}&currencies={code}&format=1&base=USD'
KEY_TEMPLATE = '{date:%Y-%m-%d}/{code}'

from_currency = params['from-currency']
to_currencies = params['to-currencies']
dst_measures = {}
measures_to_convert = params['measures']
date_column = params['date-field']
title = params.get('title')
cache = {}

fields = datapackage['resources'][0]['schema']['fields']
for measure_name in measures_to_convert:
    measure = next(filter(lambda f: f['name'] == measure_name, fields))
    for currency_code in to_currencies:
        target_name = '{}_{}'.format(measure['name'], currency_code)
        dst_measures[(measure_name, currency_code)] = target_name
        astitle = title
        if astitle is None:
            astitle = measure.get('title', measure.get('name', 'value'))
        fields.append({
            'name': target_name,
            'title': '{} ({})'.format(astitle, currency_code),
            'type': 'number'
        })


def get_rate(date, code):
    if code == 'USD':
        return 1

    params = dict(date=date, code=code, key=ACCESS_KEY)
    key = KEY_TEMPLATE.format(**params)
    if key in cache:
        rate = cache[key]
    else:
        rate = None
        url = CURRENCY_API.format(date=date, code=code, key=ACCESS_KEY)
        resp = requests.get(url)
        if resp.status_code == 200:
            resp = resp.json()
            if resp['success'] is True:
                rate = next(iter(resp.get('quotes', {}).values()))
                logging.info('%s => %s', CURRENCY_API.format(**params), rate)
        cache[key] = rate
    return rate


logged = set()


def convert(date, amount, fromcode, tocode):
    rates = []
    if type(date) in {str, int}:
        date = int(date)
        dates = [datetime.date(year=date, month=month, day=15)
                 for month in range(1, 13)]
    else:
        dates = [date]

    for date_ in dates:
        fromrate = get_rate(date_, fromcode)
        torate = get_rate(date_, tocode)
        if fromrate is not None and torate is not None:
            rate = torate / fromrate
            rates.append(rate)

    if len(rates) > 0:
        rate = sum(rates) / len(rates)

        logged_key = '%s: %s=>%s' % (date, fromcode, tocode)
        if logged_key not in logged:
            logging.info('%r -> %s', logged_key, rate)
            logged.add(logged_key)
        return str(Decimal(amount*rate).quantize(Decimal('.01'),
                                                 rounding=ROUND_DOWN))


def process_resources(_res_iter):
    def process_rows(rows):
        for row in rows:
            date = row[date_column]
            if date is not None:
                for measure_name in measures_to_convert:
                    for currency in to_currencies:
                        target_name = dst_measures[(measure_name, currency)]
                        amount = row[measure_name]
                        converted_amount = None
                        if amount is not None:
                            converted_amount = convert(date,
                                                       float(amount),
                                                       from_currency,
                                                       currency)
                        row[target_name] = converted_amount
            yield row

    for resource in _res_iter:
        yield process_rows(resource)


spew(datapackage, process_resources(res_iter))
