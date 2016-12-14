import os
import json

from datapackage_pipelines.generators import \
    GeneratorBase, SCHEDULE_DAILY, slugify, steps

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        schedule = SCHEDULE_DAILY
        title = source['title']
        dataset_name = source.get('dataset-name', title)
        dataset_name = slugify(dataset_name).lower()
        pipeline_id = dataset_name
        resource_name = source.get('resource-name', dataset_name)

        for data_source in source['sources']:
            if data_source['url'].endswith('.csv'):
                data_source['mediatype'] = 'text/csv'

        extra_measures = []
        measure_handling = []
        if 'measures' in source:
            measures = source['measures']
            measure_handling.append(('fiscal.normalise_measures',
                                     {'measures': measures['mapping']}))
            source['fields'].append({
                'header': 'value',
                'options': {
                    'currency': measures['currency']
                },
                'osType': 'value'
            })
            extra_measures = [(measure, []) for measure in source['measures']['mapping'].keys()]
            if 'currency-conversion' in measures:
                currency_conversion = measures['currency-conversion']
                date_measure = currency_conversion.get('date_measure')
                if date_measure is None:
                    date_measure = [f['header'] for f in source['fields'] if f.get('osType', '').startswith('date:')][0]
                currencies = measures.get('currencies', ['USD'])
                measure_handling.append(('fiscal.normalise_currencies',
                                         {
                                            'measures': ['value'],
                                            'date-field': date_measure,
                                            'to-currencies': currencies,
                                            'from-currency': measures['currency']
                                         }))
                for currency in currencies:
                    source['fields'].append({
                        'header': 'value_{}'.format(currency),
                        'options': {
                            'currency': currency
                        },
                        'osType': 'value'
                    })

        yield pipeline_id, schedule, steps(*[
                ('simple_remote_source',
                 {
                     'resources': source['sources']
                 }),
                ('downloader', {}, True),
                ('concat',
                 {
                     'resource-name': resource_name,
                     'column-aliases': dict([
                         (f['header'], f.get('aliases', []))
                         for f in source['fields']
                     ] + extra_measures)
                 })] + [
                (step['processor'], step.get('parameters', {}))
                for step in source.get('postprocessing', [])] +
                measure_handling + [
                ('metadata',
                 {
                     'metadata': {
                         'title': title,
                         'name': dataset_name
                     }
                 }),
                ('fiscal.model',
                 {
                     'options': dict(
                        (f['header'], f['options'])
                        for f in source['fields']
                        if 'options' in f
                     ),
                     'os-types': dict(
                        (f['header'], f['osType'])
                        for f in source['fields']
                     ),
                     'titles': dict(
                        (f['header'], f['title'])
                        for f in source['fields']
                        if 'title' in f
                     ),
                 }),
                ('dump',
                 {
                     'out-file': '{}.fdp.zip'.format(pipeline_id)
                 }),
                ('fiscal.upload',
                 {
                     'in-file': '{}.fdp.zip'.format(pipeline_id),
                     'publish': True
                 })
                ]
        )
