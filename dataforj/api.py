from dataforj.dataflow import Dataflow
from dataforj.datastep import (
    SourceStep, SinkStep, SQLStep, UnionStep, PySparkStep
)
from dataforj.envs import DataforjEnv


def init(name: str) -> Dataflow:
    return Dataflow(name, [], {})


def remove_step(flow: Dataflow, name: str):
    flow.remove_step(name)
    return flow


def add_source_step(flow: Dataflow, name: str, uri: str, format_type: str,
                    options: dict = {}):
    step = SourceStep(name=name, uri=uri, format_type=format_type,
                      options=options)
    flow.add_step(step)
    return flow


def add_sink_step(flow: Dataflow, name: str, depends_on: list, uri: str,
                  format_type: str, mode: str, options: dict = {}):
    step = SinkStep(name=name, depends_on=depends_on, uri=uri,
                    format_type=format_type, mode=mode, options=options)
    flow.add_step(step)
    return flow


def add_sql_step(flow: Dataflow, name: str, depends_on: list,
                 sql_file_path: str):
    step = SQLStep(name=name, sql_file_path=sql_file_path,
                   depends_on=depends_on)
    flow.add_step(step)
    return flow


def add_pyspark_step(flow: Dataflow, name: str, depends_on: list,
                     pyspark_file_path: str):
    step = PySparkStep(name, depends_on, pyspark_file_path)
    flow.add_step(step)
    return flow


def add_union_step(flow: Dataflow, name: str, depends_on: list):
    step = UnionStep(name, depends_on)
    flow.add_step(step)
    return flow


def run_flow(flow: Dataflow, env: str):
    env = DataforjEnv(flow.name, env)
    flow.run(env)
