import os
import json

from datapackage_pipelines.generators import \
    GeneratorBase, SCHEDULE_DAILY, slugify, steps

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.json')


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source):
        schedule = SCHEDULE_DAILY
        pipeline_id = slugify(source['title']).lower()

        for data_source in source['sources']:
            if data_source['url'].endswith('.csv'):
                data_source['mediatype'] = 'text/csv'

        yield pipeline_id, schedule, steps(*[
                ('simple_remote_source',
                 {
                     'resources': source['sources']
                 }),
                'downloader',
                ('concat',
                 {
                     'resource-name': pipeline_id,
                     'column-aliases': dict(
                         (f['header'], f['aliases'])
                         for f in source['fields']
                     )
                 })] + [
                (step['processor'], step.get('parameters', {}))
                for step in source.get('postprocessing', [])] + [
                ('metadata',
                 {
                     'metadata': {
                         'title': source['title'],
                         'name': pipeline_id
                     }
                 }),
                ('fiscal.model',
                 {
                     'options': dict(
                        (f['header'], f['options'])
                        for f in source['fields']
                        if 'options' in f
                     ),
                     'os-types': dict(
                        (f['header'], f['osType'])
                        for f in source['fields']
                     )
                 }),
                ('dump',
                 {
                     'out-file': '{}.fdp.zip'.format(pipeline_id)
                 }),
                ('fiscal.upload',
                 {
                     'in-file': '{}.fdp.zip'.format(pipeline_id),
                     'publish': True
                 })
                ]
        )
