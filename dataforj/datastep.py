def read_code_from_file(file_path, left_pad: str = '') -> str:
    with open(file_path, 'r+') as f:
        code = left_pad.join(f.readlines())
        return code


class Datastep():
    def __init__(self, name: str, depends_on: list = [], unit_tests: list = [],
                 data_quality_tests: list = [], schema: list = [], description: str = ''):
        self.name = name
        self.depends_on = depends_on
        self.unit_tests = unit_tests
        self.data_quality_tests = data_quality_tests
        self.schema = schema
        self.description = description

    def to_yaml(self) -> dict:
        pass

    def dagre_shape(self) -> dict:
        pass

    def dagre_colour(self) -> dict:
        pass

    def dagre_type(self) -> dict:
        pass

    def compile(self) -> str:
        pass


class SourceStep(Datastep):
    def __init__(self, name, uri: str, format_type: str,
                 options: dict, depends_on: list = [], unit_tests: list = [],
                 data_quality_tests: list = [], schema: list = [], description: str = ''):
        Datastep.__init__(self, name, depends_on, unit_tests,
                          data_quality_tests, schema, description)
        self.uri = uri
        self.format_type = format_type
        self.options = options

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self.depends_on == other.depends_on and \
                   self.unit_tests == other.unit_tests and \
                   self.data_quality_tests == other.data_quality_tests and \
                   self.schema == other.schema and \
                   self.uri == other.uri and \
                   self.format_type == other.format_type
        return False

    def to_yaml(self) -> dict:
        return {
            'type': 'SourceStep',
            'name': self.name,
            'depends_on': self.depends_on,
            'unit_tests': self.unit_tests,
            'data_quality_tests': self.data_quality_tests,
            'schema': self.schema,
            'description': self.description,
            'uri': self.uri,
            'format_type': self.format_type,
            'options': self.options
        }

    def compile(self) -> str:
        if self.options == None:
            options_text = ''
        else:    
            options_text = ''.join([f'.option(\'{key}\', \'{value}\')'
                                for key, value in self.options.items()])
        return f"""
{self.name}_df = spark.read{options_text}.format('{self.format_type}').load('{self.uri}') # noqa: E501
        """

    def dagre_shape(self) -> dict:
        return 'circle'

    def dagre_colour(self) -> dict:
        return 'Cyan'

    def dagre_type(self) -> dict:
        return 'Source'


class SinkStep(Datastep):
    def __init__(self, name: str, uri: str, format_type: str,
                 options: dict, mode: str, depends_on: list = [],
                 unit_tests: list = [], data_quality_tests: list = [],
                 schema: list = [], description: str = ''):
        Datastep.__init__(self, name, depends_on, unit_tests,
                          data_quality_tests, schema, description)
        self.uri = uri
        self.format_type = format_type
        self.options = options
        self.mode = mode

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self.depends_on == other.depends_on and \
                   self.unit_tests == other.unit_tests and \
                   self.data_quality_tests == other.data_quality_tests and \
                   self.schema == other.schema and \
                   self.uri == other.uri and \
                   self.format_type == other.format_type and \
                   self.mode == other.mode
        return False

    def to_yaml(self) -> dict:
        return {
            'type': 'SinkStep',
            'name': self.name,
            'depends_on': self.depends_on,
            'unit_tests': self.unit_tests,
            'data_quality_tests': self.data_quality_tests,
            'schema': self.schema,
            'description': self.description,
            'uri': self.uri,
            'format_type': self.format_type,
            'options': self.options,
            'mode': self.mode
        }

    def compile(self) -> str:
        assert len(self.depends_on) == 1, 'A Sink step must have only one dependancy'  # noqa: E501
        options_text = ''.join([f'.option(\'{key}\', \'{value}\')'
                                for key, value in self.options.items()])
        return f"""
{self.depends_on[0]}_df \
    .write{options_text} \
    .mode('{self.mode}') \
    .format('{self.format_type}') \
    .save('{self.uri}')
        """

    def dagre_shape(self) -> dict:
        return 'circle'

    def dagre_colour(self) -> dict:
        return 'DeepSkyBlue'

    def dagre_type(self) -> dict:
        return 'Sink'


class SQLStep(Datastep):
    def __init__(self, name: str, sql_file_path: str, depends_on: list = [],
                 unit_tests: list = [], data_quality_tests: list = [],
                 schema: list = [], description: str = ''):
        Datastep.__init__(self, name, depends_on, unit_tests,
                          data_quality_tests, schema, description)
        self.sql_file_path = sql_file_path
        self.sql_from_editor = None

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self.depends_on == other.depends_on and \
                   self.unit_tests == other.unit_tests and \
                   self.data_quality_tests == other.data_quality_tests and \
                   self.schema == other.schema and \
                   self.sql_file_path == other.sql_file_path and \
                   self.sql_from_editor == other.sql_from_editor
        return False

    def to_yaml(self) -> dict:
        return {
            'type': 'SQLStep',
            'name': self.name,
            'depends_on': self.depends_on,
            'unit_tests': self.unit_tests,
            'data_quality_tests': self.data_quality_tests,
            'schema': self.schema,
            'description': self.description,
            'sql_file_path': self.sql_file_path
        }

    def get_sql_code(self) -> str:
        if self.sql_from_editor == None:
            return read_code_from_file(self.sql_file_path)
        else:
            return self.sql_from_editor   

    def compile(self) -> str:
        dependencies = '\n'.join([f'{table_name}_df.registerTempTable("{table_name}")'  # noqa: E501
                                 for table_name in self.depends_on])
        return f"""
# Regiseter tables for the dependancies
{dependencies}

{self.name}_df = spark.sql(\"\"\"
{self.get_sql_code()}
\"\"\")
"""

    def dagre_shape(self) -> dict:
        return 'rect'

    def dagre_colour(self) -> dict:
        return 'Moccasin'

    def dagre_type(self) -> dict:
        return 'SQL'


class UnionStep(Datastep):
    def __init__(self, name: str, depends_on: list = [],
                 unit_tests: list = [], data_quality_tests: list = [],
                 schema: list = [], description: str = ''):
        Datastep.__init__(self, name, depends_on, unit_tests,
                          data_quality_tests, schema, description)

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self.depends_on == other.depends_on and \
                   self.unit_tests == other.unit_tests and \
                   self.data_quality_tests == other.data_quality_tests and \
                   self.schema == other.schema
        return False

    def to_yaml(self) -> dict:
        return {
            'type': 'UnionStep',
            'name': self.name,
            'depends_on': self.depends_on,
            'unit_tests': self.unit_tests,
            'data_quality_tests': self.data_quality_tests,
            'schema': self.schema,
            'description': self.description
        }

    def compile(self) -> str:
        head = self.depends_on[0]
        tail_append_str = ''.join([f'.union({df}_df)'
                                   for df in self.depends_on[1:]])
        return f"""
{self.name}_df = {head}_df{tail_append_str}
        """

    def dagre_shape(self) -> dict:
        return 'diamond'

    def dagre_colour(self) -> dict:
        return 'SandyBrown'

    def dagre_type(self) -> dict:
        return 'Union'


class PySparkStep(Datastep):
    def __init__(self, name: str, pyspark_file_path: str,
                 depends_on: list = [], unit_tests: list = [],
                 data_quality_tests: list = [], schema: list = [], description: str = ''):
        Datastep.__init__(self, name, depends_on, unit_tests,
                          data_quality_tests, schema, description)
        self.pyspark_file_path = pyspark_file_path
        self.pyspark_code_from_editor = None

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.name == other.name and \
                   self.depends_on == other.depends_on and \
                   self.unit_tests == other.unit_tests and \
                   self.data_quality_tests == other.data_quality_tests and \
                   self.schema == other.schema and \
                   self.pyspark_file_path == other.pyspark_file_path and \
                   self.pyspark_code_from_editor == other.pyspark_code_from_editor
                   
        return False

    def to_yaml(self) -> dict:
        return {
            'type': 'PySparkStep',
            'name': self.name,
            'depends_on': self.depends_on,
            'pyspark_file_path': self.pyspark_file_path,
            'unit_tests': self.unit_tests,
            'data_quality_tests': self.data_quality_tests,
            'schema': self.schema,
            'description': self.description
        }

    def get_pyspark_code(self) -> str:
        if self.pyspark_code_from_editor == None:
            return '\t' + read_code_from_file(self.pyspark_file_path, left_pad='\t')
        else:
            return self.pyspark_code_from_editor  

    def get_function_paramaters(self) -> str:
        return ', '.join([f'{name}_df' for name in self.depends_on])

    def get_function_def(self) -> str:
        return f'def dataforj_{self.name}({self.get_function_paramaters()}):'

    def compile(self) -> str:
        return f"""
{self.get_function_def()}
{self.get_pyspark_code()}

{self.name}_df = dataforj_{self.name}({self.get_function_paramaters()})
"""

    def dagre_shape(self) -> dict:
        return 'rect'

    def dagre_colour(self) -> dict:
        return 'coral'

    def dagre_type(self) -> dict:
        return 'PySpark'
