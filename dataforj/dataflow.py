import yaml
import networkx as nx
from dataforj.datastep import (
    SourceStep, SinkStep, SQLStep, UnionStep, PySparkStep
)
from dataforj.envs import DataforjEnv
from dataforj.schema import check_schema


def ut_to_pyspark(name: str, file_path: str) -> str:
    with open(file_path, 'r') as f:
        code = '\t'.join(f.readlines())
        return f"""
def dataforj_ut_{name}({name}_df):
\t{code}

dataforj_ut_{name}({name}_df)
"""


def parse_graph(steps: list) -> list:
    g = nx.DiGraph()
    for step in steps:
        g.add_node(step.name)
        if isinstance(step, SourceStep) is False:
            for dependancy in step.depends_on:
                g.add_edge(dependancy, step.name)
    # Now make sure that the graph is legal
    assert nx.is_directed_acyclic_graph(g), 'The DAG is not cyclical'
    return list(nx.topological_sort(g))


def to_dagre_nodes_edges(steps: dict) -> str:
    result = []
    for step in steps.values():
        result.append(f'g.setNode(\"{step.name}\", '
            f'{{labelType: \"html\", '
            f'label: \"<b>{step.dagre_type()}</b><br>{step.name}\", '
           f'shape: \"{step.dagre_shape()}\", '
            f'style: \"fill: {step.dagre_colour()}\"}});')
        if isinstance(step, SourceStep) is False:
            for dependancy in step.depends_on:
                result.append(f'g.setEdge(\"{dependancy}\", \"{step.name}\");')
    return "\n".join(result)


def from_yaml(yaml_text):
    yaml_contents = yaml.safe_load(yaml_text)
    return Dataflow(yaml_contents['name'], yaml_contents['graph'],
                    yaml_contents['steps'], yaml_contents['description'])


class Dataflow(object):

    def __init__(self, name: str, graph: list, step_configs: dict, description: str):
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
                    schema_location=step_config.get('schema_location', ''),
                    description=step_config.get('description', ''))
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
                    schema_location=step_config.get('schema_location', ''),
                    description=step_config.get('description', ''))
            elif step_config['type'] == 'SQLStep':
                return SQLStep(
                    name=step_config.get('name'),
                    sql_file_path=step_config.get('sql_file_path'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema_location=step_config.get('schema_location', ''),
                    description=step_config.get('description', ''))
            elif step_config['type'] == 'UnionStep':
                return UnionStep(
                    name=step_config.get('name'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema_location=step_config.get('schema_location', ''),
                    description=step_config.get('description', ''))
            elif step_config['type'] == 'PySparkStep':
                return PySparkStep(
                    name=step_config.get('name'),
                    pyspark_file_path=step_config.get('pyspark_file_path'),
                    depends_on=step_config.get('depends_on', []),
                    unit_tests=step_config.get('unit_tests', []),
                    data_quality_tests=step_config.get('data_quality_tests',
                                                       []),
                    schema_location=step_config.get('schema_location', ''),
                    description=step_config.get('description', ''))
            else:
                type_name = step_config['type']
                raise Exception(f'Unknown type [{type_name}]')

        self.name = name
        self.description = description
        self._graph = graph
        self._steps = {step_config['name']: convert(step_config)
                       for step_config in step_configs.values()}

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self.description == other.description and \
                   self._steps == other._steps and \
                   self._graph == other._graph and \
                   self.description == other.description
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
            'description': self.description,
            'graph': self._graph,
            'steps': step_configs
            }
        return yaml.dump(yaml_contents)

    def from_python_objects(name: str, description: str, steps: list):
        '''Only used for unit testing.'''
        graph = parse_graph(steps)
        step_configs = {step.name: step.to_yaml() for step in steps}
        return Dataflow(name, graph, step_configs, description)

    def run(self, env: DataforjEnv):
        exec("from pyspark.sql import SparkSession")
        exec(env.spark_session_build())
        for step_name in self._graph:
            code = self._steps[step_name].compile()
            print(f'Running step [{step_name}]')
            exec(code)
            # Validate the schema of the resulting dataframe
            if self._steps[step_name].schema_location != '':
                step_schema_location = self._steps[step_name].schema_location
                print(f'Validating step [{step_name}] against the schema located in [{step_schema_location}].')
                # The df will be accessible through the local symbol table
                check_schema(step_name, locals()[f'{step_name}_df'], step_schema_location)
            for test_path in self._steps[step_name].data_quality_tests:
                test_code = dq_to_pyspark(step_name, test_path)
                # print(test_code)
                exec(test_code)

    def unit_test(self, env: DataforjEnv, step: str = '__ALL__'):
        exec("from pyspark.sql import SparkSession")
        exec(env.spark_session_build())
        success_count = 0
        fail_count = 0
        for step_name in self._graph:
            code = self._steps[step_name].compile()
            print(f'Running step [{step_name}]')
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
            print(f'Running step [{step_name}]')
            exec(code)
            if(step_name == step):
                exec(f'{step_name}_df.show()')
                break


    def dataframe_for_step(self, env: DataforjEnv, step: str):
        exec('from pyspark.sql import SparkSession')
        exec(env.spark_session_build())
        for step_name in self._graph:
            code = self._steps[step_name].compile()
            print(f'Running step [{step_name}]')
            exec(code)
            # Validate the schema of the resulting dataframe
            if self._steps[step_name].schema_location != '':
                step_schema_location = self._steps[step_name].schema_location
                print(f'Validating step [{step_name}] against the schema located in [{step_schema_location}].')
                # The df will be accessible through the local symbol table
                check_schema(step_name, locals()[f'{step_name}_df'], step_schema_location)
            if(step_name == step):
                return locals()[f'{step_name}_df']

    def to_dagre_nodes_edges(self):
        return to_dagre_nodes_edges(self._steps)        
