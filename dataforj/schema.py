import yaml
from pyspark.sql import *

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
def dataforj_dq_not_null_{name}(df):
    from pyspark.sql.functions import col
    null_df = df.filter(col('{column_name}').isNull() == True)

    assert null_df.count() == 0, \
        f'Output of step {name} column {column_name} should not be null.'
dataforj_dq_not_null_{name}(df)
"""


def dq_accepted_values(name: str, column_name: str,
                       accepted_values: list) -> str:
    return f"""
def dq_accepted_values_{name}(df):
    from pyspark.sql.functions import col
    av_df = df \
        .filter(col('{column_name}').isin({accepted_values}) == False)
    count = av_df.count()
    values = av_df.select(col('{column_name}')).distinct() \
        .rdd.map(lambda row : row[0]).collect()
    assert count == 0, \
        f'Output of step [{name}] column [{column_name}] should only have ' \
        f'values in the accepted list [{', '.join(accepted_values)}]. ' \
        f'These values were also found {{values}}.'

dq_accepted_values_{name}(df)
"""

def check_schema(step_name: str, df: DataFrame, schema_yaml_location: str):
    '''Open a schema file, parse it and then do the actual schema validation'''
    with open(schema_yaml_location, 'r') as f:
        yaml_text = '\n'.join(f.readlines())
        schema_yaml = yaml.safe_load(yaml_text)
        check_schema_yaml(step_name, df, schema_yaml)

def check_schema_yaml(step_name: str, df: DataFrame, schema_yaml):
    '''Iterate through all columns in a schema and validate the df'''
    for column in schema_yaml:
        for test in column['tests']:
            column_name = column['name']
            if isinstance(test, str):
                print(f'Running data quality check [{test}] '
                      f'for column [{column_name}]')
                if test == 'not_null':
                    exec(dq_not_null(step_name, column_name), locals(), globals())
            elif isinstance(test, dict):
                if 'accepted_values' in test:
                    accepted_values = test['accepted_values']
                print(f'Running data quality check [accepted_values] '
                        f'for column [{column_name}]')
                exec(dq_accepted_values(step_name, column_name,
                        accepted_values))