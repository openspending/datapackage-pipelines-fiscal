from datapackage_pipelines.generators import slugify

from .utils import extract_names, extract_storage_ids
from datapackage_pipelines_fiscal.processors.consts import ID_COLUMN_NAME


def normalized_flow(source, base):

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
    db_tables = dict(
        (res, '{}_{}'.format(db_table, i))
        for i, res in enumerate(resources)
    )
    db_tables[''] = db_table

    deps = [
        'dimension_flow_{}'.format(res)
        for res in resources
        ]

    steps = [
        ('load_metadata', {
            'url': 'dependency://' + base + '/denormalized_flow',
        }),
    ]
    steps.extend([
        ('load_resource', {
            'url': 'dependency://' + base + '/' + dep,
            'resource': resource
        })
        for resource, dep in zip(resources, deps)
    ])
    steps.extend([
        ('load_resource', {
            'url': 'dependency://' + base + '/denormalized_flow',
            'resource': resource_name
        }),
        ('fiscal.create_babbage_model', {
            'db-tables': db_tables
        }),
    ])
    for resource, kind in zip(resources, kinds):
        headers = [
            f['header']
            for f in source['fields']
            if f['columnType'].startswith(kind+':') or f['columnType'] == kind
        ]
        steps.extend([
            ('join', {
                'source': {
                    'name': resource,
                    'key': headers,
                    'delete': True
                },
                'target': {
                    'name': resource_name,
                    'key': headers
                },
                'fields': {
                    resource + '_id': {
                        'name': ID_COLUMN_NAME
                    }
                }
            }),
            ('delete_fields', {
                'resources': resource_name,
                'fields': headers
            }),
        ])
    steps.extend([
        ('add_metadata', {
            'savedPk': [resource + '_id' for resource in resources]
        }),
        ('fiscal.helpers.load_primarykey', {}),
        ('fiscal.update_model_in_registry', {
            'dataset-id': dataset_id,
            'loaded': False
        }),
        ('dump.to_path', {
            'out-path': 'normalized/final'
        })
    ])
    yield steps, deps + ['denormalized_flow'], ''
