import os

from .utils import extract_names, extract_storage_ids

BUCKET = os.environ.get('S3_BUCKET_NAME')


def dump_for_openspending_flow(source, base):
    '''
    Dump an OpenSpending compatible datapackage to AWS S3. This ensures a basic
    OS editable package is available.
    '''

    _, _, resource_name = extract_names(source)
    dataset_id, _, dataset_path = extract_storage_ids(source)

    pipeline_steps = [
        (
            'load_metadata',
            {
                'url': 'dependency://' + base + '/denormalized_flow',
            }
        ),
        (
            'load_resource',
            {
                'url': 'dependency://' + base + '/denormalized_flow',
                'resource': resource_name
            }
        )
     ]

    if BUCKET is not None:
        pipeline_steps.extend([
            (
                'aws.dump.to_s3',
                {
                    'bucket': BUCKET.split('/')[-1],
                    'path': dataset_path,
                    'pretty-descriptor': True
                }
            ),
        ])
    else:
        pipeline_steps.extend([
            (
                'dump.to_zip',
                {
                    'out-file': '{}_final.zip'.format(dataset_id),
                    'pretty-descriptor': True
                }
            ),
        ])

    yield pipeline_steps, ['denormalized_flow'], ''
