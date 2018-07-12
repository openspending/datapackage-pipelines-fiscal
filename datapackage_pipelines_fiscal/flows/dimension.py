from datapackage_pipelines.generators import slugify
from .utils import extract_names


def dimension_flow(source, base):

    title, dataset_name, resource_name = extract_names(source)

    kinds = sorted(set(
        f['columnType'].split(':')[0]
        for f in source['fields']
    ) - {'value'})

    resources = [
        slugify(kind, separator='_')
        for kind in kinds
    ]

    pipeline_ids = [
        'dimension_{}'.format(res)
        for res in resources
        ]

    for resource, pipeline_id, kind in zip(resources, pipeline_ids, kinds):
        headers = [
            f['header']
            for f in source['fields']
            if f['columnType'].startswith(kind+':') or f['columnType'] == kind
        ]
        steps = [
            ('load_resource', {
                'url': 'dependency://' + base + '/denormalized_flow',
                'resource': resource_name
            }),
            ('concatenate', {
                'target': {
                    'name': resource
                },
                'fields': dict(
                    (h, [])
                    for h in headers
                )
            }),
            ('fiscal.helpers.save_primarykey', ),
            ('join', {
                'source': {
                    'name': resource,
                    'key': headers,
                    'delete': True
                },
                'target': {
                    'name': resource,
                    'key': None
                },
                'fields': dict(
                    (h, None)
                    for h in headers
                )
            }),
            ('fiscal.helpers.load_primarykey', ),
            ('fiscal.helpers.enumerate', ),
            ('dump.to_path', {
                'out-path': 'normalized/'+resource
            })
        ]
        yield steps, ['denormalized_flow'], resource
