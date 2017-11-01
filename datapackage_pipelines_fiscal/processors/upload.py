import json
import os
import zipfile
import tempfile

from datapackage_pipelines.wrapper import ingest, spew

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

    temp_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(params['in-file']) as in_file:
        for name in in_file.namelist():
            in_file.extract(name, temp_dir)

    datapackage_json = os.path.join(temp_dir, 'datapackage.json')
    datapackage = json.load(open(datapackage_json))
    datapackage['count_of_rows'] = line_count
    json.dump(datapackage, open(datapackage_json, 'w'))

    package = gobble.fiscal.FiscalDataPackage(datapackage_json, user=user)
    package.upload(skip_validation=True, publish=publish)


spew(datapackage, line_counter(res_iter), finalizer=run)
