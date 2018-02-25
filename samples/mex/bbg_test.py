import json
import os
import sqlalchemy
import logging

from babbage.cube import Cube
from babbage.model import Measure

logging.root.setLevel(logging.INFO)

base = os.path.dirname(__file__)

engine = sqlalchemy.create_engine(os.environ.get('DPP_DB_ENGINE', 'postgresql://osuser:1234@localhost/os'),
                                  echo=False)
model = json.load(open(os.path.join(base, 'normalized/final/datapackage.json')))['babbageModel']
# model['fact_table'] = 'normalized'
cube = Cube(engine, 'normalized', model)
logging.info('Getting 20 facts')
facts = cube.facts(page_size=20)

logging.info('total_fact_count=%s', facts['total_fact_count'])
logging.info('keys=%s', list(facts.keys()))

members = cube.members('ID_CLAVE_CARTERA', cuts='CICLO:2017', page_size=500)
logging.info('members #=%s', len(members['data']))
logging.info('total_member_count=%s', members['total_member_count'])
logging.info('members sample=\n%r', members['data'][:10])

# print(facts)