import copy
import logging
import csv
import tempfile
import unittest.mock as mock
from datapackage import DataPackage as Package
from datapackage_pipelines.wrapper import ingest, spew


def run(parameters):
    assert 'in-file' in parameters, 'Missing required parameter "in-file"'
    assert 'out-file' in parameters, 'Missing required parameter "out-file"'

    return split_resource_per_year(
        parameters['in-file'],
        parameters['out-file']
    )


def split_resource_per_year(in_file, out_file):
    dp = Package(in_file)

    if not _is_valid(dp):
        logging.warn(
            'DataPackage doesn\'t comply with our prerequisites. Ignoring it.'
        )
    else:
        resource = dp.resources[0]
        resources_per_year = _split_rows_per_year(resource)

        if len(resources_per_year.keys()) <= 1:
            logging.info(
                'Skipping creation of resources per year,'
                ' as there is only data for a single fiscal year'
            )
        else:
            default_resource_descriptor = _clean_resource_descriptor(resource)

            for year, resource_data in sorted(resources_per_year.items(), reverse=True):  # noqa: E501
                # Make sure all rows are written to the filesystem
                resource_data['fp'].flush()

                descriptor = copy.deepcopy(default_resource_descriptor)
                descriptor.update({
                    'name': str(year),
                    'path': resource_data['fp'].name,
                    'count_of_rows': resource_data['count_of_rows'],
                    'profile': 'tabular-data-resource',
                })
                dp.descriptor['resources'].append(descriptor)

                logging.info(
                    'Created resource for year %s (%d rows)',
                    year,
                    resource_data['count_of_rows']
                )

    # FIXME: Remove this whenever we're able to create and safe datapackages
    # without safe resources.
    # See https://github.com/frictionlessdata/datapackage-py/issues/171
    with mock.patch('datapackage.helpers.is_safe_path', return_value=True):
        dp.commit()
        dp.save(out_file)

    return dp


def _is_valid(dp):
    num_resources = len(dp.resources)
    if num_resources != 1:
        logging.warn(f'There can only be 1 resource (found {num_resources})')
        return False

    if _get_fiscal_year_field(dp.resources[0]) is None:
        return False

    return True


def _split_rows_per_year(resource):
    fiscal_year_field = _get_fiscal_year_field(resource)
    if not fiscal_year_field:
        return

    resources_per_year = {}
    for row in resource.iter(keyed=True, cast=False):
        fiscal_year = row.get(fiscal_year_field['name'])
        if not fiscal_year:
            continue

        if fiscal_year not in resources_per_year:
            fp = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8')
            fieldnames = row.keys()
            writer = csv.DictWriter(fp, fieldnames=fieldnames)
            writer.writeheader()

            resources_per_year[fiscal_year] = {
                'writer': writer,
                'fp': fp,
                'count_of_rows': 0,
            }

        resources_per_year[fiscal_year]['writer'].writerow(row)
        resources_per_year[fiscal_year]['count_of_rows'] += 1

    logging.info(
        'Created %d resources for years %s',
        len(resources_per_year.keys()),
        resources_per_year.keys()
    )

    return resources_per_year


def _get_fiscal_year_field(resource):
    schema = resource.descriptor.get('schema', {})
    fields = schema.get('fields', [])
    fiscal_year_type = 'date:fiscal-year'

    fiscal_year_fields = [
        field for field in fields
        if field.get('osType') == fiscal_year_type
    ]

    if not fiscal_year_fields:
        logging.warn(
            f'Could not find a field with osType equal to "{fiscal_year_type}"'
        )
    elif len(fiscal_year_fields) > 1:
        msg = (
            f'There can be only one field with the fiscal year'
            f' (found {len(fiscal_year_fields)})'
        )
        logging.warn(msg)
    else:
        return fiscal_year_fields[0]


def _clean_resource_descriptor(resource):
    descriptor = copy.deepcopy(resource.descriptor)
    data_fields = [
        'name',
        'bytes',
        'count_of_rows',
        'hash',

        'data',
        'path',
        'url',
    ]

    for field in data_fields:
        if field in descriptor:
            del descriptor[field]

    return descriptor


if __name__ == '__main__':
    parameters, datapackage, resource_iterator = ingest()

    spew(
        datapackage,
        resource_iterator,
        finalizer=lambda: run(parameters)
    )
