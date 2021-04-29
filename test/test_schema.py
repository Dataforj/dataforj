import unittest
import yaml
from pyspark.sql import *
from dataforj import schema
from test.test_samples import flow_simple, simple_yaml_text, flow_complex


city_yaml_text = '''
- name: city
  tests: 
    - not_null
    - accepted_values: ['Amsterdam', 'Dublin', 'Frankfurt']
 '''  

flag_yaml_text = '''
- name: flag
  tests: 
    - not_null
 '''  

combined_yaml_text = f'{city_yaml_text}\n{flag_yaml_text}'

spark = SparkSession \
    .builder \
    .appName("Unit Test") \
    .getOrCreate()

ut_step_df = spark.createDataFrame(
    [
        ('Amsterdam', True),  
        ('Dublin', False),
    ],
    ['city', 'flag'] 
)

ut_bad_city_df = spark.createDataFrame(
    [
        ('Amsterdam', True),  
        ('New York', False)
    ],
    ['city', 'flag'] 
)

ut_bad_flag_df = spark.createDataFrame(
    [
        ('Amsterdam', True),  
        ('New York', None)
    ],
    ['city', 'flag'] 
)

class SchemaTest(unittest.TestCase):

    def test_schema_two_fields(self):
        '''Test to make sure two fields can be validated in the same YAML'''
        schema_yaml = yaml.safe_load(combined_yaml_text)
        schema.check_schema_yaml('ut_step_df', ut_step_df, schema_yaml)

    def test_positive_accepted_values(self):
        schema_yaml = yaml.safe_load(city_yaml_text)
        schema.check_schema_yaml('ut_step_df', ut_step_df, schema_yaml)

    def test_negative_accepted_values(self):
        schema_yaml = yaml.safe_load(city_yaml_text)
        with self.assertRaises(AssertionError) as excinfo:
            schema.check_schema_yaml('ut_bad_city_df', ut_bad_city_df, schema_yaml)
        self.assertEqual('Output of step [ut_bad_city_df] column [city] should only have values '
                         'in the accepted list [Amsterdam, Dublin, Frankfurt]. These values '
                         'were also found [\'New York\'].', 
                         str(excinfo.exception))

    def test_positive_null(self):
        schema_yaml = yaml.safe_load(flag_yaml_text)
        schema.check_schema_yaml('ut_step_df', ut_step_df, schema_yaml)

    def test_negative_accepted_values(self):
        schema_yaml = yaml.safe_load(flag_yaml_text)
        with self.assertRaises(AssertionError) as excinfo:
            schema.check_schema_yaml('ut_bad_flag_df', ut_bad_flag_df, schema_yaml)
        self.assertEqual('Output of step ut_bad_flag_df column flag should not be null.', 
                         str(excinfo.exception))

    def test_check_schema(self):
        '''Test to make sure we can read in the YAML file from the example project'''
        schema.check_schema('ut_step_df', ut_step_df, 'example/schemas/filter_schema.yaml')
                         
        

if __name__ == "__main__":
    SchemaTest().test_positive_null()
