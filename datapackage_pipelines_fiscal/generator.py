import os
import json

from datapackage_pipelines.generators import \
    GeneratorBase, slugify, steps

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        title = source['title']
        dataset_name = source.get('dataset-name', title)
        dataset_name = slugify(dataset_name).lower()
        pipeline_id = dataset_name
        resource_name = source.get('resource-name', dataset_name)

        for data_source in source['sources']:
            if data_source['url'].endswith('.csv'):
                data_source['mediatype'] = 'text/csv'
            if 'name' not in data_source:
                data_source['name'] = slugify(
                    os.path.basename(data_source['url'])
                )

        model_params = {
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
        }
        extra_measures = []
        measure_handling = []
        if 'measures' in source:
            measures = source['measures']
            normalise_measures = ('fiscal.normalise_measures', {
                'measures': measures['mapping']
            })
            if 'title' in measures:
                normalise_measures[1]['title'] = measures['title']
            measure_handling.append(normalise_measures)
            model_params['os-types']['value'] = 'value'
            model_params['options']['value'] = {
                'currency': measures['currency']
            }
            extra_measures = [
                (measure, [])
                for measure in source['measures']['mapping'].keys()
            ]
            if 'currency-conversion' in measures:
                currency_conversion = measures['currency-conversion']
                date_measure = currency_conversion.get('date_measure')
                if date_measure is None:
                    date_measure = [
                        f['header']
                        for f in source['fields']
                        if f.get('osType', '').startswith('date:')
                    ][0]
                currencies = measures.get('currencies', ['USD'])
                normalise_currencies = ('fiscal.normalise_currencies', {
                    'measures': ['value'],
                    'date-field': date_measure,
                    'to-currencies': currencies,
                    'from-currency': measures['currency']
                })
                if 'title' in currency_conversion:
                    normalise_currencies[1]['title'] = measures['title']
                measure_handling.append(normalise_currencies)
                for currency in currencies:
                    measure_name = 'value_{}'.format(currency)
                    model_params['os-types'][measure_name] = 'value'
                    model_params['options'][measure_name] = {
                        'currency': currency
                    }

        dedpulicate_lines = source.get('deduplicate') is True
        dedpulicate_steps = []
        if dedpulicate_lines:
            dedpulicate_steps.append((
                'join',
                {
                    'source': {
                        'name': resource_name,
                        'key': [
                            f['header']
                            for f in source['fields']
                            if f['osType'] != 'value'
                        ]
                    },
                    'target': {
                        'name': resource_name,
                        'key': None
                    },
                    'fields': dict(
                        (f['header'],
                         {
                             'name': f['header'],
                             'aggregate': 'any' if f['osType'] != 'value' else 'sum'
                         })
                        for f in source['fields']
                    )
                }
            ))


        partial_output_file = '{}.fdp.partial.zip'.format(pipeline_id)
        output_file = '{}.fdp.zip'.format(pipeline_id)
        pipeline_steps = [
            (
                'add_metadata',
                {
                   'title': title,
                   'name': dataset_name,
                }
            )
        ] + [
            ('add_resource', source)
            for source in source['sources']
        ] + [
            ('stream_remote_resources', {}, True),
            ('concatenate', {
                'target': {
                    'name': resource_name
                },
                'fields': dict(
                    [
                        (f['header'], f.get('aliases', []))
                        for f in source['fields']
                    ] + extra_measures
                )
            }),
        ] + [
            (step['processor'], step.get('parameters', {}))
            for step in source.get('postprocessing', [])
        ] + measure_handling + [
            ('fiscal.model', model_params),
            ('dump.to_zip', {
                'out-file': partial_output_file,
            }),
            ('fiscal.split_resource_per_fiscal_year_and_dump_to_zip', {
                'in-file': partial_output_file,
                'out-file': output_file,
            }),
            ('fiscal.upload', {
                'in-file': output_file,
                'publish': True
            }),
        ]

        pipeline_details = {
            'pipeline': steps(*pipeline_steps),
        }
        yield pipeline_id, pipeline_details
