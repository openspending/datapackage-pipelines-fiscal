from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

columns = params.get('columns')


def clean_value(v):
    if v == '-':
        return '0'
    return v


def process_resources(_res_iter):
    for rows in _res_iter:
        def process_rows(_rows):
            for row in _rows:
                yield dict((k, v if k not in columns else clean_value(v)) for (k, v)
                           in row.items())
        yield process_rows(rows)

spew(datapackage, process_resources(res_iter))
