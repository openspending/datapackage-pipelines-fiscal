import os
import copy

from datapackage_pipelines.wrapper import process
from os_package_registry import PackageRegistry

ES_ADDRESS = os.environ.get('ELASTICSEARCH_ADDRESS')


def modify_datapackage(dp, parameters, *_):
    dataset_id = parameters['dataset-id']
    loaded = parameters.get('loaded')
    private = parameters.get('private')
    datapackage_url = parameters.get('datapackage-url')
    if ES_ADDRESS:
        registry = PackageRegistry(ES_ADDRESS)
        datapackage = copy.deepcopy(dp)
        params = {}
        if 'babbageModel' in datapackage:
            model = datapackage['babbageModel']
            del datapackage['babbageModel']
            params['model'] = model
        if private is not None:
            datapackage['private'] = private
        if datapackage_url:
            params['datapackage_url'] = datapackage_url
            params['datapackage'] = datapackage
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
