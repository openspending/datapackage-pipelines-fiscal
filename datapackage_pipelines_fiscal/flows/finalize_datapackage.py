import os
import logging

from .utils import extract_names, extract_storage_ids

BUCKET = os.environ.get('S3_BUCKET_NAME')
logging.info('DUMPING results to BUCKET %s', BUCKET)


def finalize_datapackage_flow(source):

    _, _, resource_name = extract_names(source)
    _, _, dataset_path = extract_storage_ids(source)

    pipeline_steps = [
                         (
                             'load_metadata',
                             {
                                 'url': 'dependency://./denormalized_flow',
                             }
                         ),
                         (
                             'load_resource',
                             {
                                 'url': 'dependency://./denormalized_flow',
                                 'resource': resource_name
                             }
                         ),
                         (
                             'fiscal.split_per_fiscal_year'
                         ),
                     ]
    if BUCKET is not None:
        pipeline_steps.extend([
            (
                'aws.dump.to_s3',
                {
                    'bucket': BUCKET,
                    'path': '{}/final'.format(dataset_path),
                    'pretty-descriptor': True
                }
            ),
            ('fiscal.update_model_in_registry', {
                'dataset-id': dataset_id,
                'datapackage-url': 'https://{}/{}/final/datapackage.json'.format(BUCKET, dataset_path)
            }),
        ])
    else:
        pipeline_steps.append(
            (
                'dump.to_path',
                {
                    'out-path': 'final'
                }
            )
        )
        

    yield pipeline_steps, ['./denormalized_flow'], ''
