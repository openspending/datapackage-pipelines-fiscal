import copy
import collections
import os

from datapackage import DataPackage as Package
from datapackage_pipelines.wrapper import (
    ingest,
    spew,
    get_dependency_datapackage_url)
from datapackage_pipelines.lib.dump.file_formats import CSVFormat
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def split_to_years(res, fields, router):
    fiscal_year_field = [k for k, v in fields.items()
                         if v['columnType'] == 'date:fiscal-year'
                         ][0]
    csv_format = CSVFormat()
    for row in res:
        val = row[fiscal_year_field]
        writer = router.get(val)
        if writer is not None:
            csv_format.write_row(writer, row, fields)
        yield row
    for writer in router.values():
        csv_format.finalize_file(writer)


def process_resources(res_iter, fields, router):
    first = next(res_iter)
    yield split_to_years(first, fields, router)
    for res in res_iter:
        collections.deque(res, 0)


if __name__ == '__main__':
    parameters, datapackage, res_iter = ingest()

    denormalized_pkg = Package(get_dependency_datapackage_url(
        parameters['source-pipeline'][13:]))
    denormalized = denormalized_pkg.resources[0]
    fiscal_years = list(filter(lambda r: r.name == 'fiscal-years',
                               denormalized_pkg.resources))

    if len(fiscal_years) == 0:
        spew(
            datapackage,
            res_iter
        )
    else:
        fiscal_years = fiscal_years[0]
        fiscal_years = list(map(lambda x: x[0], fiscal_years.iter()))
        name_prefix = denormalized.name
        datapackage['resources'] = datapackage['resources'][:1]
        fields = denormalized.descriptor['schema']['fields']
        headers = [f['name'] for f in fields]
        fields = dict(zip(headers, fields))
        router = {}
        for year in fiscal_years:
            descriptor = copy.deepcopy(denormalized.descriptor)
            if PROP_STREAMING in descriptor:
                del descriptor[PROP_STREAMING]
            descriptor['name'] = '{}__{}'.format(name_prefix, year)
            descriptor['path'] = 'data/{}__{}.csv'.format(name_prefix, year)
            datapackage['resources'].append(descriptor)
            out_filename = os.path.join('final', descriptor['path'])
            os.makedirs(os.path.dirname(out_filename), exist_ok=True)
            out_file = open(out_filename, 'w')
            router[year] = CSVFormat().initialize_file(out_file, headers)

        spew(datapackage, process_resources(res_iter, fields, router))
