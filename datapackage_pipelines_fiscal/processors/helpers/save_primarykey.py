from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):
    dp['savedPk'] = dp['resources'][0]['schema']['primaryKey']
    return dp


process(modify_datapackage=modify_datapackage)