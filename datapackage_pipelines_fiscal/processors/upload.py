import json
import os
import zipfile
import tempfile

from datapackage_pipelines.wrapper import ingest, spew
import datapackage_pipelines_fiscal.helpers as helpers

import gobble

params, datapackage, res_iter = ingest()

line_count = 0


def line_counter(_res_iter):
    def process_rows(_rows):
        global line_count
        for row in _rows:
            line_count += 1
            yield row

    for res in _res_iter:
        yield process_rows(res)


def run():
    user = gobble.user.User()
    publish = params.get('publish', False)
    in_filename = open(params['in-file'], 'rb')

    in_file = zipfile.ZipFile(in_filename)
    temp_dir = tempfile.mkdtemp()
    for name in in_file.namelist():
        in_file.extract(name, temp_dir)
        in_file.close()
        datapackage_json = os.path.join(temp_dir, 'datapackage.json')
        datapackage = json.load(open(datapackage_json))
        datapackage['count_of_rows'] = line_count
        json.dump(datapackage, open(datapackage_json, 'w'))

        package = gobble.fiscal.FiscalDataPackage(datapackage_json, user=user)
        package.upload(skip_validation=True, publish=publish)


# Wrap iterator using helpers.run_after_yielding_elements() so we
# guarantee that this processor is run after every processor before
# it has finished, but will block subsequent processors that use this
# wrapper from running until it's finished.
iterator = helpers.run_after_yielding_elements(
    line_counter(res_iter),
    lambda: run()
)
spew(datapackage, iterator)
