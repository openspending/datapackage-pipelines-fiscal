import os
import copy

from datapackage_pipelines.wrapper import process
from os_package_registry import PackageRegistry

ES_ADDRESS = os.environ.get('ELASTICSEARCH_ADDRESS')


def modify_datapackage(dp, parameters, *_):
    dataset_id = parameters['dataset-id']
    loaded = parameters.get('loaded')
    datapackage_url = parameters.get('datapackage-url')
    if ES_ADDRESS:
        registry = PackageRegistry(ES_ADDRESS)
        params = {}
        if 'babbageModel' in dp:
            model = dp['babbageModel']
            datapackage = copy.deepcopy(dp)
            del datapackage['babbageModel']
            params.update(dict(
                model=model,
                datapackage=datapackage
            ))
        if datapackage_url:
            params['datapackage_url'] = datapackage_url
        if loaded is not None:
            params['loaded'] = loaded
            params['loading_status'] = 'done' if loaded else 'loading-data'
        registry.update_model(
            dataset_id,
            **params
        )
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage)
