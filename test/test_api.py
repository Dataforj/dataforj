import unittest
from dataforj.dataflow import Dataflow
import dataforj.api as api
from test.test_samples import flow_complex


class APITest(unittest.TestCase):

    def test_init(self) -> Dataflow:
        self.assertEqual(api.init(name='name', description='desc'), 
                         Dataflow(name='name', graph=[], step_configs={}, description='desc'))

    def test_all(self):
        flow = api.init('complex', 'complex')

        api.add_source_step(flow=flow, name='s1', uri='fake_file_path',
                            format_type='csv', options={})
        api.add_source_step(flow=flow, name='s2', uri='fake_file_path',
                            format_type='csv', options={})
        api.add_sql_step(flow=flow, name='sql1', depends_on=['s1'],
                         sql_file_path='my_file.sql')
        api.add_sql_step(flow=flow, name='sql2', depends_on=['s2'],
                         sql_file_path='my_file.sql')
        api.add_union_step(flow=flow, name='union', 
                           depends_on=['sql1', 'sql2'])
        api.add_union_step(flow=flow, name='union_remove',
                           depends_on=['sql1', 'sql2'])
        api.add_sql_step(flow=flow, name='sql3', depends_on=['union'],
                         sql_file_path='my_file.sql')
        api.add_sql_step(flow=flow, name='sql4', depends_on=['union'],
                         sql_file_path='my_file.sql')
        api.add_sink_step(flow=flow, name='sink1', depends_on=['sql3'],
                          uri='fake_file_path', format_type='csv',
                          options={}, mode='overwrite')
        api.add_sink_step(flow=flow, name='sink2', depends_on=['sql4'],
                          uri='fake_file_path', format_type='csv',
                          options={}, mode='overwrite')
        api.remove_step(flow=flow, name='union_remove')
        api_names = [str(step) for step in flow._steps]
        complex_names = [str(step) for step in flow_complex._steps]
        self.assertEqual(api_names, complex_names)


if __name__ == "__main__":
    APITest().test_all()
