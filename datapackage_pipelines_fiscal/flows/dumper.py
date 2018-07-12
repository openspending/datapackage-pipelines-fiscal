from datapackage_pipelines.generators import slugify

from .utils import extract_names, extract_storage_ids


def dumper_flow(source, base):

    _, _, resource_name = extract_names(source)
    dataset_id, db_table, _ = extract_storage_ids(source)

    kinds = sorted(set(
        f['columnType'].split(':')[0]
        for f in source['fields']
    ) - {'value'})

    resources = [
        slugify(kind, separator='_')
        for kind in kinds
    ]

    deps = [
        'dimension_flow_{}'.format(res)
        for res in resources
        ]

    for i, resource, dep, kind in zip(range(len(kinds)), resources, deps,
                                      kinds):
        res_db_table = '{}_{}'.format(db_table, i)
        steps = [
            ('load_resource', {
                'url': 'dependency://' + base + '/' + dep,
                'resource': resource
            }),
            ('set_types',),
            ('fiscal.helpers.fix_null_pks',),
            ('dump.to_sql', {
                'tables': {
                    res_db_table: {
                        'resource-name': resource
                    }
                }
            })
        ]
        yield steps, [dep], resource

    steps = [
        ('load_resource', {
            'url': 'dependency://' + base + '/normalized_flow',
            'resource': resource_name
        }),
        ('fiscal.helpers.fix_null_pks',),
        ('dump.to_sql', {
            'tables': {
                db_table: {
                    'resource-name': resource_name
                }
            }
        })
    ]
    yield steps, ['normalized_flow'], ''

    yield [
        ('fiscal.update_model_in_registry', {
            'dataset-id': dataset_id,
            'loaded': True
        }),
    ], ['dumper_flow'], 'update_status'
