import yaml
import networkx as nx
from dataforj.datastep import (
    SourceStep, SinkStep, SQLStep, UnionStep, PySparkStep
)
from dataforj.envs import DataforjEnv


def ut_to_pyspark(name: str, file_path: str) -> str:
    with open(file_path, 'r') as f:
        code = '\t'.join(f.readlines())
        return f"""
def dataforj_ut_{name}({name}_df):
\t{code}

dataforj_ut_{name}({name}_df)
"""


def dq_to_pyspark(name: str, file_path: str) -> str:
    with open(file_path, 'r') as f:
        code = '\t'.join(f.readlines())
        return f"""
def dataforj_dq_{name}({name}_df):
\t{code}

dataforj_dq_{name}({name}_df)
"""


def dq_not_null(name: str, column_name: str) -> str:
    return f"""
def dataforj_dq_not_null_{name}({name}_df):
    from pyspark.sql.functions import col
    null_df = {name}_df.filter(col('{column_name}').isNull() == True)

    assert null_df.count() == 0, \
        f'Output of step {name} column {column_name} should be not null, '
dataforj_dq_not_null_{name}({name}_df)
"""


def dq_accepted_values(name: str, column_name: str,
                       accepted_values: list) -> str:
    return f"""
def dataforj_dq_not_null_{name}({name}_df):
    from pyspark.sql.functions import col
    av_df = {name}_df \
        .filter(col('{column_name}').isin({accepted_values}) == False)
    count = av_df.count()
    values = av_df.select(col('{column_name}')).distinct() \
        .rdd.map(lambda row : row[0]).collect()
    assert count == 0, \
        f'Output of step [{name}] column [{column_name}] should only have ' \
        f'values in the accepted list [{', '.join(accepted_values)}]. ' \
        f'These values were also found {{values}}.'

dataforj_dq_not_null_{name}({name}_df)
"""


def parse_graph(steps: list) -> list:
    g = nx.DiGraph()
    for step in steps:
        g.add_node(step.name)
        if isinstance(step, SourceStep) is False:
            g.add_node(step.name)
            for dependancy in step.depends_on:
                g.add_edge(dependancy, step.name)
    # Now make sure that the graph is legal
    assert nx.is_directed_acyclic_graph(g), 'The DAG is not cyclical'
    return list(nx.topological_sort(g))


def from_yaml(yaml_text):
    yaml_contents = yaml.safe_load(yaml_text)
    return Dataflow(yaml_contents['name'], yaml_contents['graph'],
                    yaml_contents['steps'])


class Dataflow(object):

    def __init__(self, name: str, graph: list, step_configs: dict):
        def convert(step_config: dict):
            if step_config['type'] == 'SourceStep':
                return SourceStep(
                    name=step_config.get('name'),
                    uri=step_config.get('uri'),
                    format_type=step_config.get('format_type'),
                    options=step_config.get('options'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema=step_config.get('schema', []))
            elif step_config['type'] == 'SinkStep':
                return SinkStep(
                    name=step_config.get('name'),
                    uri=step_config.get('uri'),
                    format_type=step_config.get('format_type'),
                    options=step_config.get('options'),
                    mode=step_config.get('mode'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema=step_config.get('schema', []))
            elif step_config['type'] == 'SQLStep':
                return SQLStep(
                    name=step_config.get('name'),
                    sql_file_path=step_config.get('sql_file_path'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema=step_config.get('schema', []))
            elif step_config['type'] == 'UnionStep':
                return UnionStep(
                    name=step_config.get('name'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema=step_config.get('schema', []))
            elif step_config['type'] == 'PySparkStep':
                return PySparkStep(
                    name=step_config.get('name'),
                    pyspark_file_path=step_config.get('pyspark_file_path'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema=step_config.get('schema', []))
            else:
                type_name = step_config['type']
                raise Exception(f'Unknown type [{type_name}]')

        self.name = name
        self._graph = graph
        self._steps = {step_config['name']: convert(step_config)
                       for step_config in step_configs.values()}

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self._steps == other._steps and \
                   self._graph == other._graph
        return False

    def add_step(self, step):
        self._steps[step.name] = step
        self._graph = parse_graph(self._steps.values())

    def remove_step(self, step_name):
        del self._steps[step_name]
        self._graph = parse_graph(self._steps.values())

    def to_yaml(self) -> str:
        step_configs = {step_name: self._steps[step_name].to_yaml()
                        for step_name in self._graph}
        yaml_contents = {
            'name': self.name,
            'graph': self._graph,
            'steps': step_configs
            }
        return yaml.dump(yaml_contents)

    def from_python_objects(name: str, steps: list):
        graph = parse_graph(steps)
        step_configs = {step.name: step.to_yaml() for step in steps}
        return Dataflow(name, graph, step_configs)

    def run(self, env: DataforjEnv):
        exec("from pyspark.sql import SparkSession")
        exec(env.spark_session_build())
        for step_name in self._graph:
            code = self._steps[step_name].compile()
            # print(code)
            exec(code)
            for test_path in self._steps[step_name].data_quality_tests:
                test_code = dq_to_pyspark(step_name, test_path)
                # print(test_code)
                exec(test_code)
            for column in self._steps[step_name].schema:
                for test in column['tests']:
                    column_name = column['name']
                    if isinstance(test, str):
                        print(f'Running data quality check [{test}] '
                              f'for column [{column_name}]')
                        if test == 'not_null':
                            exec(dq_not_null(step_name, column_name))
                    elif isinstance(test, dict):
                        if 'accepted_values' in test:
                            accepted_values = test['accepted_values']
                        print(f'Running data quality check [accepted_values] '
                              f'for column [{column_name}]')
                        exec(dq_accepted_values(step_name, column_name,
                             accepted_values))

    def unit_test(self, env: DataforjEnv, step: str = '__ALL__'):
        exec("from pyspark.sql import SparkSession")
        exec(env.spark_session_build())
        success_count = 0
        fail_count = 0
        for step_name in self._graph:
            code = self._steps[step_name].compile()
            # print(code)
            exec(code)
            if(step == '__ALL__' or step_name == step) and not \
              (isinstance(self._steps[step_name], SourceStep) or
               isinstance(self._steps[step_name], SinkStep)):
                if len(self._steps[step_name].unit_tests) == 0:
                    print(f'Step [{step_name}] has no unit tests')
                else:
                    success_count_step = 0
                    fail_count_step = 0
                    for test_path in self._steps[step_name].unit_tests:
                        test_code = ut_to_pyspark(step_name, test_path)
                        try:
                            # print(test_code)
                            exec(test_code)
                            success_count_step = success_count_step + 1
                        except Exception as e:
                            print(f'Step [{step_name}] test [{test_path}] '
                                  f' failed with error [{e}]')
                            fail_count_step = fail_count_step + 1
                    print(f'For step [{step_name}] [{success_count_step}] '
                          f'tests passed and [{fail_count_step}] tests '
                          f'failed.')
                    success_count = success_count + success_count_step
                    fail_count = fail_count + fail_count_step
                if(step_name == step):
                    break

        print(f'Overall result: [{success_count}] '
              f'tests passed and [{fail_count}] tests '
              f'failed.')

    def debug_step(self, env: DataforjEnv, step: str):
        exec('from pyspark.sql import SparkSession')
        exec(env.spark_session_build())
        for step_name in self._graph:
            code = self._steps[step_name].compile()
            # print(code)
            exec(code)
            if(step_name == step):
                exec(f'{step_name}_df.show()')
                break
