import unittest
import os
from dataforj.dataflow import Dataflow
import dataforj.cli as cli
import dataforj.dataflow as dataflow

dir_path = '/tmp/dataforj_ut'
file_name = '/tmp/dataforj_ut/dataforj.yaml'


class CLITest(unittest.TestCase):

    def test_init(self):
        cli.init(dir_path, 'ut')
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            flow2 = Dataflow.from_python_objects('ut', [])
            self.assertEqual(flow._steps, flow2._steps)

    def setUp(self):
        if not os.path.exists(dir_path):
            path = os.path.join('/tmp/', 'dataforj_ut')
            os.mkdir(path)

    def tearDown(self):
        import shutil
        try:
            shutil.rmtree(dir_path)
        except OSError as e:
            print("Error: %s : %s" % (dir_path, e.strerror))


if __name__ == "__main__":
    CLITest().tearDown()
    CLITest().setUp()
    CLITest().test_init()
