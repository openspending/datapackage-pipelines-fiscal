import os
import copy

from datapackage_pipelines.wrapper import process
from os_package_registry import PackageRegistry


def modify_datapackage(dp, parameters, *_):
    dataset_id = parameters['dataset-id']
    env_var_name = 'ELASTICSEARCH_ADDRESS'
    if env_var_name in os.environ:
        es_connection_string = os.environ[env_var_name]
        registry = PackageRegistry(es_connection_string)
        model = dp['babbageModel']
        datapackage = copy.deepcopy(dp)
        del datapackage['babbageModel']
        registry.update_model(
            dataset_id,
            model=model,
            datapackage=datapackage
        )
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage)