from dataforj.dataflow import Dataflow
from dataforj.datastep import (
    SourceStep, SQLStep, SinkStep, UnionStep
)

flow_simple = Dataflow.from_python_objects(name='simple', description='simple', steps=[
                SourceStep(name='sample', uri='fake_file_path',
                           format_type='csv', options={}),
                SQLStep(name='grouper', depends_on=['sample'],
                        sql_file_path='my_file.sql'),
                SinkStep(name='sinker', depends_on=['grouper'],
                         uri='fake_file_path', format_type='csv',
                         options={}, mode='overwrite')])
flow_complex = Dataflow.from_python_objects(name='complex', description='complex', steps=[
                SourceStep(name='s1', uri='fake_file_path', format_type='csv',
                           options={}),
                SourceStep(name='s2', uri='fake_file_path', format_type='csv',
                           options={}),
                SQLStep(name='sql1', depends_on=['s1'],
                        sql_file_path='my_file.sql'),
                SQLStep(name='sql2', depends_on=['s2'],
                        sql_file_path='my_file.sql'),
                UnionStep(name='union', depends_on=['sql1', 'sql2']),
                SQLStep(name='sql3', depends_on=['union'],
                        sql_file_path='my_file.sql'),
                SQLStep(name='sql4', depends_on=['union'],
                        sql_file_path='my_file.sql'),
                SinkStep(name='sink1', depends_on=['sql3'],
                         uri='fake_file_path', format_type='csv', options={},
                         mode='overwrite'),
                SinkStep(name='sink2', depends_on=['sql4'],
                         uri='fake_file_path', format_type='csv',
                         options={}, mode='overwrite')])

simple_yaml_text = '''description: simple
graph:
- sample
- grouper
- sinker
name: simple
steps:
  grouper:
    data_quality_tests: []
    depends_on:
    - sample
    description: ''
    name: grouper
    schema: []
    sql_file_path: my_file.sql
    type: SQLStep
    unit_tests: []
  sample:
    data_quality_tests: []
    depends_on: []
    description: ''
    format_type: csv
    name: sample
    options: {}
    schema: []
    type: SourceStep
    unit_tests: []
    uri: fake_file_path
  sinker:
    data_quality_tests: []
    depends_on:
    - grouper
    description: ''
    format_type: csv
    mode: overwrite
    name: sinker
    options: {}
    schema: []
    type: SinkStep
    unit_tests: []
    uri: fake_file_path
'''