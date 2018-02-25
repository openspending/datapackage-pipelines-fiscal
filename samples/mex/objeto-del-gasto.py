#encoding: utf8
import os
import csv

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

columns = params.get('columns')


new_columns = [
    'ID_PARTIDA_GENERICA',
    'ID_PARTIDA_ESPECIFICA',
    'DESC_PARTIDA_ESPECIFICA',
    'DESC_PARTIDA_GENERICA',
    'DESC_CAPITULO',
]

field_names = [f['name'] for f in datapackage['resources'][0]['schema']['fields']]

for column in new_columns:
    if column not in field_names:
        datapackage['resources'][0]['schema']['fields'].append({
            'name': column,
            'type': 'string'
        })


lookup_map = {}
for kind in ['partida_específica',  'partida_generica',
             'capitulo', 'concepto']:
    reader = csv.reader(open(os.path.join('data', 'objeto_del_gasto',  kind+'.csv')))
    next(reader)
    for row in reader:
        key, value, *_ = row
        lookup_map.setdefault(kind, {})[key] = value


def lookup(key, catalog, year):
    return lookup_map[catalog].get(key.strip())


def process_row(row):
    year = int(row['CICLO'])

    # Skip the LAST year of the dataset (currently 2016) it has split columns already
    if year >= 2017:
        return row

    objeto = row['ID_CONCEPTO']
    if objeto:
        row['ID_CAPITULO'] = objeto[0] + '000'
        row['ID_CONCEPTO'] = objeto[:2] + '00'
        row['DESC_CAPITULO'] = lookup(row['ID_CAPITULO'], 'capitulo', year)
        row['DESC_CONCEPTO'] = lookup(row['ID_CONCEPTO'], 'concepto', year)

        nb_generica_digits = 4 if year in (2008, 2009, 2010) else 3

    if objeto and len(objeto)>=4:
        row['ID_PARTIDA_GENERICA'] = objeto[:nb_generica_digits]

    row['DESC_PARTIDA_GENERICA'] = lookup(row.get('ID_PARTIDA_GENERICA'), 'partida_generica', year)

    if year not in (2008, 2009, 2010):
        if objeto and len(objeto) >= 5:
            row['ID_PARTIDA_ESPECIFICA'] = objeto
            row['DESC_PARTIDA_ESPECIFICA'] = lookup(row.get('ID_PARTIDA_ESPECIFICA'), 'partida_específica', year)

    return row


def process_resources(_res_iter):
    for rows in _res_iter:
        def process_rows(_rows):
            for row in _rows:
                yield process_row(row)
        yield process_rows(rows)

spew(datapackage, process_resources(res_iter))
