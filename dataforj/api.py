from dataforj.dataflow import Dataflow
from dataforj.datastep import (
    SourceStep, SinkStep, SQLStep, UnionStep, PySparkStep
)
from dataforj.envs import DataforjEnv


def init(name: str, description: str) -> Dataflow:
    return Dataflow(name, [], {}, description)


def remove_step(flow: Dataflow, name: str):
    flow.remove_step(name)
    return flow


def add_source_step(flow: Dataflow, name: str, uri: str, format_type: str,
                    options: dict = {}, description=""):
    step = SourceStep(name=name, uri=uri, format_type=format_type,
                      options=options, description=description)
    flow.add_step(step)
    return flow


def add_sink_step(flow: Dataflow, name: str, depends_on: list, uri: str,
                  format_type: str, mode: str, options: dict = {}, description=""):
    step = SinkStep(name=name, depends_on=depends_on, uri=uri,
                    format_type=format_type, mode=mode, options=options)              
    flow.add_step(step)
    return flow


def add_sql_step(flow: Dataflow, name: str, depends_on: list,
                 sql_file_path: str, description=""):
    step = SQLStep(name=name, sql_file_path=sql_file_path,
                   depends_on=depends_on)
    flow.add_step(step)
    return flow


def add_pyspark_step(flow: Dataflow, name: str, depends_on: list,
                     pyspark_file_path: str, description=""):
    step = PySparkStep(name=name, depends_on=depends_on, pyspark_file_path=pyspark_file_path)
    flow.add_step(step)
    return flow


def add_union_step(flow: Dataflow, name: str, depends_on: list, description=""):
    step = UnionStep(name, depends_on=depends_on, description=description)
    flow.add_step(step)
    return flow


def run_flow(flow: Dataflow, env: str):
    env = DataforjEnv(flow.name, env)
    flow.run(env)


def save_flow(file_name: str, flow: Dataflow):
    with open(file_name, 'w') as f:
        f.write(flow.to_yaml())
