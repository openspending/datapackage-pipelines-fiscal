from datapackage_pipelines.wrapper import process

from datapackage_pipelines_fiscal.processors.consts import ID_COLUMN_NAME


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    row[ID_COLUMN_NAME] = row_index + 1
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].insert(0, {
        'name': ID_COLUMN_NAME,
        'type': 'integer'
    })
    dp['resources'][0]['schema']['primaryKey'].insert(0, ID_COLUMN_NAME)
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)