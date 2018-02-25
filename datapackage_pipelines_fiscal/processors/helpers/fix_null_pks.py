from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    for pk in spec['schema']['primaryKey']:
        if row.get(pk) is None: row[pk] = ''
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['missingValues'] = ['NULLNULLNULLNULL']
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)