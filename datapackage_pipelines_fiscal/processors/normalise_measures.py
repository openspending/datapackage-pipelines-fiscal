import copy

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

measures = params['measures']
title = params.get('title', 'value')
all_keys = set.union(*[set(measure.keys()) for measure in measures.values()])


def validate_keys(_measures):
    for measure_name, measure in _measures.items():
        measure_keys = set(measure.keys())
        missing_keys = all_keys - measure_keys
        if len(missing_keys) > 0:
            msg = 'Missing keys for measure {}: {}'.format(
                measure_name,
                missing_keys
            )
            raise RuntimeError(msg)
        for value in measure.values():
            if type(value) is not str:
                msg = 'Values measure {} should all be strings {}'.format(
                    measure_name,
                    value
                )
                raise RuntimeError(msg)


def amend_datapackage(_datapackage, _measures):
    fields = _datapackage['resources'][0]['schema']['fields']
    fields = [f for f in fields if f['name'] not in _measures.keys()]
    fields.append({
        'name': 'value',
        'title': title,
        'type': 'number'
    })
    field_names = [f['name'] for f in fields]
    for key in all_keys:
        if key not in field_names:
            fields.append({
                              'name': key,
                              'type': 'string'
                          })
    _datapackage['resources'][0]['schema']['fields'] = fields


def process_resources(_res_iter, _measures):
    def process_rows(rows):
        for row in rows:
            for measure_name, measure_value in _measures.items():
                row_copy = copy.copy(row)
                row_copy['value'] = row_copy[measure_name]
                row_copy.update(measure_value)
                for _measure_name in _measures.keys():
                    del row_copy[_measure_name]
                yield row_copy

    for resource in _res_iter:
        yield process_rows(resource)


validate_keys(measures)
amend_datapackage(datapackage, measures)
spew(datapackage, process_resources(res_iter, measures))
