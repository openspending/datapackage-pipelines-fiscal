import datetime
import logging

import requests
from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal, ROUND_DOWN

params, datapackage, res_iter = ingest()

CURRENCY_API = 'http://currencies.apps.grandtrunk.net/getrate/{date:%Y-%m-%d}/{fromcode}/{tocode}'
KEY_TEMPLATE = '{date:%Y-%m-%d}/{fromcode}/{tocode}'

from_currency = params['from-currency']
to_currencies = params['to-currencies']
dst_measures = {}
measures_to_convert = params['measures']
date_column = params['date-field']
cache = {}

fields = datapackage['resources'][0]['schema']['fields']
for measure_name in measures_to_convert:
    measure = next(filter(lambda f:f['name'] == measure_name, fields))
    for currency_code in to_currencies:
        target_name = '{}_{}'.format(measure['name'], currency_code)
        dst_measures[(measure_name, currency_code)] = target_name
        fields.append({
            'name': target_name,
            'title': '{} ({})'.format(measure['name'], currency_code),
            'type': 'number'
        })


def convert(date, amount, fromcode, tocode):
    params = dict(date=date, tocode=tocode, fromcode=fromcode)
    key = KEY_TEMPLATE.format(**params)
    if key in cache:
        rate = cache[key]
    else:
        rate = requests.get(CURRENCY_API.format(**params))
        if rate.status_code == 200:
            logging.info('%s => %s', CURRENCY_API.format(**params), rate.text)
            rate = Decimal(rate.text)
        else:
            rate = None
        cache[key] = rate
        params['rate'] = rate
        logging.info('RATE {fromcode}->{tocode} @ {date:%Y-%m-%d} = {rate}'.format(**params))

    if rate is not None:
        return str((Decimal(amount)*rate).quantize(Decimal('.01'),
                                                   rounding=ROUND_DOWN))


def process_resources(_res_iter):
    def process_rows(rows):
        for row in rows:
            date = row[date_column]
            if type(date) is str:
                date = int(date)
            if type(date) is int:
                date = datetime.date(year=date, month=1, day=1)
            if date is not None:
                for measure_name in measures_to_convert:
                    for currency in to_currencies:
                        target_name = dst_measures[(measure_name, currency)]
                        amount = row[measure_name]
                        converted_amount = None
                        if amount is not None:
                            converted_amount = convert(date,
                                                       amount,
                                                       from_currency,
                                                       currency)
                        row[target_name] = converted_amount
            yield row

    for resource in _res_iter:
        yield process_rows(resource)


spew(datapackage, process_resources(res_iter))