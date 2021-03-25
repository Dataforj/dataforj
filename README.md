# Dataforj

Dataforj is a tool to easily create data models with good engineering principles, such as text based code, unit tests, data quality checks and a seperation of the development and runtime environments.  Data Analysts and Data Engineers are able to work on the same code base to create reliable projects that can be run on any Spark cluster.

## Features

- runs on any Spark cluster
- supports multiple enviroments
- CLI
- text based code
- data quality checks
- unit tests

## How Dataforj works

Dataforj is composed of a CLI which can be used to run Dataforj models.  And a project strucuture which can be composed by hand, or by tooling, which makes it easy to build and maintain complex models with good engineering principles.  

> TODO: Add a diagram showing Data Analyst working on SQL in a project, a Data Engineer working on PySpark on the same project, both having private git repos, and after git the project getting pushed to test/prd envs by the DevOps pipeline.

A Dataforj model is composed of steps which are chained together and executed in the correct order.  The currently supported list of step types is:

- Source
- Sink
- Union
- SQL
- PySpark

> TODO: more steps can be added.  E.g. filter, split, etc.  Perhaps some basic ML steps.  With a good UI, there should be a visual SQL editor which would result in an SQL step that is only defined in the YAML.

## Runs on any Spark Cluster

Dataforj can connect to and run on any Spark cluster or service.  Each environment has its own configuration YAML which specifies how to connect to the cluster in that environment.  

## Environments

One of the goals of Dataforj is to make it simple to run models on different environments easily.  Each project has an env folder where YAML files with the config for each environment are located.  This YAML file can be used to specify the connection to the Spark cluster for the environment, and environment specific values that will be replaced in the project YAML (e.g. the location where a Source step should pick up the data).

## CLI

The Dataforj CLI can be isntalled anywhere using pip.  This means it can easily integrate with DevOps tooling.

pip install dataforj (not yet working)

It can be used in development environments to create and update projects.  E.g.

```bash
dataforj -p /tmp/example -n demo init
dataforj -p /tmp/example -s transactions -t source add
dataforj -p /tmp/example -s customers -t source add
dataforj -p /tmp/example -s customers_sql -t sql -d customers add
```

etc.

And in runtime environments the CLI can be used to debug, test and run a model.  E.g.

```bash
dataforj -p /tmp/example --step filter --env local debug-step
dataforj -p /tmp/example --step filter --env local unit-test
dataforj -p /tmp/example --env local run
```

Note: the ability to create a project and update it (init, add commands) from the CLI should be superceeded by the ability to do that through a web UI.

## Text based code

Each Dataforj project has a project YAML file which specifies meta data about the steps and how all the steps in the model should be chained together.  Some steps only require meta data to be executed (e.g. Source, Sink, Union steps) while other steps have code in external files which are referenced in the meta data (e.g. SQL and PySpark steps).

The Dataforj project YAML can be created by hand in a text editor or in a UI which produces the YAML.  The project YAML is easily code review-able.

## Data quality checks

There are two types of data quality checks.  The first is schema validation.  You can apply schema rules and basic qualtiy checks in the Dataforj YAML.  E.g.

```yaml
    schema:
      - name: city
        tests: 
          - not_null
          - accepted_values: ['Amsterdam', 'Dublin', 'Frankfurt']
      - name: flag
        tests: 
          - not_null
```

The second is via a custom Python script that will be executed after the step has completed.  You can expect a DataFrame named in the format {step_name}_df to be available to this script.

```python
from pyspark.sql.functions import col, lit

assert filter_df.filter(col('city') == lit('Amsterdam')).count() > 0, \
       'There must be customers in Amsterdam'
```

## Unit tests

Unit tests will test that a certain outcome is expected for a certain environment.  For example, you can have an env called "ut" which will define some input data for your unit tests, and then you can define test cases for any step you choose that will test that the expected outcome is present.  E.g.

```python
from pyspark.sql.functions import col, lit

assert filter_df.filter(col('flag') == lit(False)).count() == 0, \
       '0 rows with flag false should be returned'
```

Once again, you can expect a DataFrame named in the format {step_name}_df to be available to this script.

> TODO: Maybe this should be changed, maybe the unit test should define it's own input data.

## Source / Sink steps

Source and Sink steps are defined in the the Dataforj YAML.  The format type (e.g. CSV, Parquet, JSON, etc), the URI (i.e. the location of the data) as well as any custom options are defined there.  For Sinks the write mode (e.g. "overwrite") must also be defined.  For Source steps the standard data qualityc checks can be applied.

## SQL step

For an SQL step there must be an SQL file which contains the code to be executed.  The YAML config for the SQL step will specify the path to the file.  The YAML config will specify the steps that the SQL step depends on, and these will be available as input tables.  In the example below, the SQL step depends on the customers_latest and transactions_with_products steps, and these are available as tables to use in the SQL.  The type of steps that the SQL step depends on do not need to be SQL steps, they can be any step.

```sql
select c.id, c.name, c.city, t.date, t.amount, t.flag 
from customers_latest c, transactions_with_products t
where c.id = t.customer_id
```

The output of the SQL step will be the result of the SQL.

## PysPark step

For a PySpark step there must be a Python file which contains the code to be executed.  The YAML config for the PySpark step will specify the path to the file.  The YAML config will specify the steps that the PySpark step depends on, and these will be available as input DataFrames.  You can expect the DataFrame to be named in the format {step_name}_df.   A PySpark step can depend on many prior steps, so can have many inout DataFrames.   The type of steps that the PySpark step depends on do not need to be PySpark steps, they can be any step.

```python
from pyspark.sql.functions import col, lit

return result_df.filter(col('flag') == lit(True))
```

At the end of a PySpark step you must return a DataFrame with the output for that step.

## Union Step

For a union step the dependent steps need be listed in the Dataforj YAML.  The output of this step will be the union of all dependants.
