from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    row['id'] = row_index + 1
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].insert(0, {
        'name': 'id',
        'type': 'integer'
    })
    dp['resources'][0]['schema']['primaryKey'].insert(0, 'id')
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)