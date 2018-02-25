import os

from .utils import extract_names


def finalize_datapackage_flow(source):

    title, dataset_name, resource_name = extract_names(source)

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
                         (
                             'dump.to_path',
                             {
                                 'out-path': 'final'
                             }
                         )
                     ]

    yield pipeline_steps, ['./denormalized_flow'], ''
