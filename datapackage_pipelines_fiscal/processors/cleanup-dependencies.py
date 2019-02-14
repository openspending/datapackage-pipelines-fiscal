import os
import shutil
import logging

from datapackage_pipelines.wrapper import process


def cleanup(dp, parameters, *_):
    dir_to_clean = parameters['dirs_to_clean']
    for dir_name in dir_to_clean:
        abs_path = os.path.abspath(dir_name)
        logging.info('Cleaning artifact: {}'.format(abs_path))
        try:
            shutil.rmtree(abs_path)
        except FileNotFoundError:
            logging.warning('No artifact to clean: {}'.format(abs_path))

    return dp


if __name__ == '__main__':
    process(modify_datapackage=cleanup)
