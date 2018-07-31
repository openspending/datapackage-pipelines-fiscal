import os

from .utils import extract_names, extract_storage_ids

from datapackage_pipelines.generators import slugify


def denormalized_flow(source, base):

    title, dataset_name, resource_name = extract_names(source)
    dataset_id, _, _ = extract_storage_ids(source)

    original_datapackage_url = source.get('datapackage-url')

    for data_source in source['sources']:
        if data_source['url'].endswith('.csv'):
            data_source['mediatype'] = 'text/csv'
        if 'name' not in data_source:
            data_source['name'] = slugify(
                os.path.basename(data_source['url']),
                separator='_'
            ).lower()

    model_params = {
        'options': dict(
            (f['header'], f['options'])
            for f in source['fields']
            if 'options' in f
        ),
        'os-types': dict(
            (f['header'], f['columnType'])
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
                    if f.get('columnType', '').startswith('date:')
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
            'set_types',
            {
                'types': dict(
                    (f['header'],
                     dict(
                         type='number',
                         **f.get('options', {})
                     )
                     )
                    for f in source['fields']
                    if f['columnType'] == 'value'
                )
            }
        ))
        dedpulicate_steps.append((
            'join',
            {
                'source': {
                    'name': resource_name,
                    'key': [
                        f['header']
                        for f in source['fields']
                        if f['columnType'] != 'value'
                        ],
                    'delete': True
                },
                'target': {
                    'name': resource_name,
                    'key': None
                },
                'fields': dict(
                    (f['header'],
                     {
                         'name': f['header'],
                         'aggregate': 'any' if f['columnType'] != 'value' else 'sum'  # noqa
                     })
                    for f in source['fields']
                )
            }
        ))

    load_metadata_steps = []
    if original_datapackage_url:
        load_metadata_steps.append((
            'load_metadata', {
                'url': original_datapackage_url
            }
        ))

    pipeline_steps = load_metadata_steps + [
                         (
                             'add_metadata',
                             {
                                 'title': title,
                                 'name': dataset_name,
                                 'revision': source.get('revision', 0),
                             }
                         ),
                         ('fiscal.update_model_in_registry', {
                             'dataset-id': dataset_id,
                             'loaded': False
                         }),
                     ] + [
                         ('add_resource', source)
                         for source in source['sources']
                         ] + [
                         ('stream_remote_resources', {}),
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
                     ] + dedpulicate_steps + [
                         (step['processor'], step.get('parameters', {}))
                         for step in source.get('postprocessing', [])
                     ] + measure_handling + [
                         ('fiscal.model', model_params),
                         ('fiscal.collect-fiscal-years', ),
                         ('set_types',),
                         ('dump.to_path', {
                             'out-path': 'denormalized',
                         }),
                     ]

    yield pipeline_steps, [], ''
