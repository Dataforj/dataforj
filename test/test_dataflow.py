import unittest
from dataforj.dataflow import Dataflow
from dataforj.envs import DataforjEnv
import dataforj.dataflow as dataflow
from dataforj.datastep import (
    SourceStep, SQLStep, SinkStep, UnionStep, PySparkStep
)
from test.test_samples import flow_simple, simple_yaml_text, flow_complex


class DataflowTest(unittest.TestCase):

    def test__init_graph_simple(self):
        self.assertEqual(flow_simple._graph, ['sample', 'grouper',
                                              'sinker'])

    def test__init_graph_complex(self):
        self.assertEqual(flow_complex._graph,
                         ['s2', 'sql2', 's1', 'sql1', 'union',
                          'sql4', 'sink2', 'sql3', 'sink1'])

    def test__init_graph_exception(self):
        with self.assertRaises(AssertionError) as excinfo:
            Dataflow.from_python_objects('exception', [
                SourceStep(name='sample', uri='fake_file_path',
                           format_type='csv', options={}),
                SinkStep(name='failer', depends_on=['failer'],
                         uri='fake_file_path', format_type='csv',
                         options={}, mode='overwrite')])
        self.assertEqual('The DAG is not cyclical', str(excinfo.exception))

    def test_to_yaml(self):
        self.assertEqual(flow_simple.to_yaml(),
                         simple_yaml_text)

    def test_from_yaml(self):
        yaml_flow = dataflow.from_yaml(simple_yaml_text)
        self.assertEqual(yaml_flow.to_yaml(), flow_simple.to_yaml())

    def test_add_step(self):
        flow = Dataflow.from_python_objects('complex', [])
        flow.add_step(SourceStep(name='s1', uri='fake_file_path',
                                 format_type='csv', options={}, ))
        flow.add_step(SourceStep(name='s2', uri='fake_file_path',
                                 format_type='csv', options={}, ))
        flow.add_step(SQLStep(name='sql1', depends_on=['s1'],
                              sql_file_path='my_file.sql'))
        flow.add_step(SQLStep(name='sql2', depends_on=['s2'],
                              sql_file_path='my_file.sql'))
        flow.add_step(UnionStep(name='union', depends_on=['sql1', 'sql2']))
        flow.add_step(SQLStep(name='sql4', depends_on=['union'],
                              sql_file_path='my_file.sql'))
        flow.add_step(SQLStep(name='sql3', depends_on=['union'],
                              sql_file_path='my_file.sql'))
        flow.add_step(SinkStep(name='sink1', depends_on=['sql3'],
                               uri='fake_file_path', format_type='csv',
                               options={}, mode='overwrite'))
        flow.add_step(SinkStep(name='sink2', depends_on=['sql4'],
                               uri='fake_file_path', format_type='csv',
                               options={}, mode='overwrite'))
        self.assertEqual(flow._steps, flow_complex._steps)

    def test_add_step_reversed(self):
        flow = Dataflow.from_python_objects('complex', [])
        flow.add_step(SinkStep(name='sink2', depends_on=['sql4'],
                               uri='fake_file_path', format_type='csv',
                               options={}, mode='overwrite'))
        flow.add_step(SinkStep(name='sink1', depends_on=['sql3'],
                               uri='fake_file_path', format_type='csv',
                               options={}, mode='overwrite'))
        flow.add_step(SQLStep(name='sql4', depends_on=['union'],
                              sql_file_path='my_file.sql'))
        flow.add_step(SQLStep(name='sql3', depends_on=['union'],
                              sql_file_path='my_file.sql'))
        flow.add_step(UnionStep(name='union', depends_on=['sql1', 'sql2']))
        flow.add_step(SQLStep(name='sql2', depends_on=['s2'],
                              sql_file_path='my_file.sql'))
        flow.add_step(SQLStep(name='sql1', depends_on=['s1'],
                              sql_file_path='my_file.sql'))
        flow.add_step(SourceStep(name='s2', uri='fake_file_path',
                                 format_type='csv', options={}, ))
        flow.add_step(SourceStep(name='s1', uri='fake_file_path',
                                 format_type='csv', options={}, ))
        self.assertEqual(flow._steps, flow_complex._steps)

    def test_run(self):
        flow = Dataflow.from_python_objects('example data', [
                SourceStep(name='customers', uri='example/data/customers.csv',
                           format_type='csv', options={'header': 'true'}),
                SourceStep(name='products', uri='example/data/products.csv',
                           format_type='csv', options={'header': 'true'}),
                SourceStep(name='transactions',
                           uri='example/data/transactions.csv',
                           format_type='csv', options={'header': 'true'}),
                SQLStep(name='customers_latest',
                        sql_file_path='example/sql/customers_latest.sql',
                        depends_on=['customers']),
                SQLStep(name='transactions_with_products',
                        depends_on=['products', 'transactions'],
                        sql_file_path='example/sql/transactions_with_products.sql'),  # noqa: E501
                SQLStep(name='result',
                        sql_file_path='example/sql/result.sql',
                        depends_on=['transactions_with_products',
                                    'customers_latest']),
                PySparkStep(name='filter',
                            pyspark_file_path='example/pyspark/filter.py',
                            depends_on=['result'],
                            schema=[{'name': 'city',
                                    'tests': [
                                        'not_null',
                                        {'accepted_values':
                                            ['Amsterdam',
                                             'Frankfurt',
                                             'Dublin']}
                                        ]
                                     }]),
                SinkStep(name='sink', uri='example/data/result.csv',
                         format_type='csv', options={'header': 'true'},
                         mode='overwrite', depends_on=['filter'])])
        env = DataforjEnv(flow.name, 'local')
        flow.run(env)
        # TODO - this ut has no assert


if __name__ == "__main__":
    DataflowTest().test_run()
