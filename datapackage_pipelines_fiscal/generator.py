import os
import json

from datapackage_pipelines.generators import GeneratorBase, steps

from .flows.denormalized import denormalized_flow
from .flows.dump_for_openspending import dump_for_openspending_flow
from .flows.dumper import dumper_flow
from .flows.dimension import dimension_flow
from .flows.normalized import normalized_flow
from .flows.finalize_datapackage import finalize_datapackage_flow


SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.json')

FLOWS = [
    denormalized_flow,
    dumper_flow,
    dimension_flow,
    normalized_flow,
    finalize_datapackage_flow,
]

OS_FLOWS = [
    dump_for_openspending_flow
]


class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source, base):

        all_pipeline_ids = []

        for flow in FLOWS:
            for pipeline_steps, deps, suffix in flow(source, base):
                pipeline_id = base + '/' + flow.__name__
                if suffix:
                    pipeline_id += '_' + suffix
                pipeline_details = {
                    'pipeline': steps(*pipeline_steps),
                    'dependencies': [
                        dict(pipeline=base + '/' + dep)
                        for dep in deps
                    ]
                }
                all_pipeline_ids.append(pipeline_id)
                yield pipeline_id, pipeline_details

        if not source.get('suppress-os', False):
            for flow in OS_FLOWS:
                for pipeline_steps, deps, suffix in flow(source, base):
                    pipeline_id = base + '/' + flow.__name__
                    if suffix:
                        pipeline_id += '_' + suffix
                    pipeline_details = {
                        'pipeline': steps(*pipeline_steps),
                        'dependencies': [
                            dict(pipeline=base + '/' + dep)
                            for dep in deps
                        ]
                    }
                    all_pipeline_ids.append(pipeline_id)
                    yield pipeline_id, pipeline_details

        # clean up dependencies if keep-artifacts is not True.
        if not source.get('keep-artifacts', False):
            dirs_to_clean = ["denormalized", "normalized", "final"]
            pipeline_id = base + '/' + 'cleanup-dependencies'
            pipeline_details = {
                'pipeline':
                    steps(('fiscal.cleanup-dependencies',
                          {'dirs_to_clean': dirs_to_clean})),
                'dependencies': [{'pipeline': dep} for dep in all_pipeline_ids]
            }
            yield pipeline_id, pipeline_details
