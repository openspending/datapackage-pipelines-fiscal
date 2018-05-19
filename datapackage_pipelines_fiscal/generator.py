import os
import json

from datapackage_pipelines.generators import \
    GeneratorBase, slugify, steps

from .flows.denormalized import denormalized_flow
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

class Generator(GeneratorBase):

    @classmethod
    def get_schema(cls):
        return json.load(open(SCHEMA_FILE))

    @classmethod
    def generate_pipeline(cls, source, base):

        for flow in FLOWS:
            for pipeline_steps, deps, suffix in flow(source, base):
                pipline_id = base + '/' + flow.__name__
                if suffix:
                    pipline_id += '_' + suffix
                pipeline_details = {
                    'pipeline': steps(*pipeline_steps),
                    'dependencies': [
                        dict(
                            pipeline = base + '/' + dep
                        )
                        for dep in deps
                    ]
                }
                yield pipline_id, pipeline_details
