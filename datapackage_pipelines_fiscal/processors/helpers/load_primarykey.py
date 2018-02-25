from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):
    fieldnames = set(f['name'] for f in dp['resources'][0]['schema']['fields'])
    dp['resources'][0]['schema']['primaryKey'] = list(filter(
        lambda a: a in fieldnames, dp['savedPk']
    ))
    del dp['savedPk']
    return dp




process(modify_datapackage=modify_datapackage)