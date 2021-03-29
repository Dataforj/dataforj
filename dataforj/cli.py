import os
import dataforj.api as api
import dataforj.dataflow as dataflow
from dataforj.envs import DataforjEnv


def open_flow(dir: str):
    '''
    open a project in the directory provided
    '''
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            return flow


def init(dir: str, name: str):
    '''
    init a new project in the directory provided
    '''
    file_name = f'{dir}/dataforj.yaml'
    if os.path.exists(file_name):
        raise Exception(
            f'There is aleady a Dataforj project in the directory [{dir}]'
            )
    else:
        flow = api.init(name)
        if not os.path.exists(os.path.dirname(file_name)):
            try:
                os.makedirs(os.path.dirname(file_name))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        with open(file_name, 'w') as writer:
            writer.write(flow.to_yaml())
        return flow    


def remove_step(dir: str, name: str):
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            updated_flow = api.remove_step(flow, name)
            f.seek(0)
            f.write(updated_flow.to_yaml())
            f.truncate()


def add_step(dir: str, name: str, step_type: str, depends_on_list: str):
    depends_on = depends_on_list.split(',')
    if step_type == 'source':
        add_source_step(dir, name,
                        f'{{{name}_uri}}', f'{{{name}_format_type}}')
    elif step_type == 'sink':
        add_sink_step(dir, name, depends_on,
                      f'{{{name}_uri}}', f'{{{name}_format_type}}')
    elif step_type == 'union':
        add_union_step(dir, name, depends_on)
    elif step_type == 'sql':
        add_sql_step(dir, name, 
                     depends_on=depends_on,
                     sql_file_path=f'sql/{name}.sql')
    elif step_type == 'pyspark':
        add_pyspark_step(dir, name, depends_on, f'sql/{name}.sql')
    else:
        raise Exception(f'Unknonw step type [{step_type}]')


def add_source_step(dir: str, name: str, uri: str, format_type: str):
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            updated_flow = api.add_source_step(flow, name, uri, format_type)
            f.seek(0)
            f.write(updated_flow.to_yaml())
            f.truncate()


def add_sink_step(dir: str, name: str, depends_on: list, uri: str,
                  format_type: str):
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            updated_flow = api.add_sink_step(flow, name, depends_on, uri,
                                             format_type)
            f.seek(0)
            f.write(updated_flow.to_yaml())
            f.truncate()


def add_sql_step(dir: str, name: str, depends_on: list, sql_file_path: str):
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            updated_flow = api.add_sql_step(flow=flow, name=name,
                                            depends_on=depends_on,
                                            sql_file_path=sql_file_path)
            f.seek(0)
            f.write(updated_flow.to_yaml())
            f.truncate()


def add_pyspark_step(dir: str, name: str, depends_on: list,
                     pyspark_file_path: str):
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            updated_flow = api.add_pyspark_step(flow, name, depends_on,
                                                pyspark_file_path)
            f.seek(0)
            f.write(updated_flow.to_yaml())
            f.truncate()


def add_union_step(dir: str, name: str, depends_on: list):
    file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(file_name):
        raise Exception('There is no Dataforj project in this directory')
    else:
        with open(file_name, 'r+') as f:
            yaml = '\n'.join(f.readlines())
            flow = dataflow.from_yaml(yaml)
            updated_flow = api.add_union_step(flow, name, depends_on)
            f.seek(0)
            f.write(updated_flow.to_yaml())
            f.truncate()


def run(dir: str, env_name: str):
    project_file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(project_file_name):
        raise Exception(f'There is no Dataforj project in the directory \
                          [{dir}]')
    else:
        with open(project_file_name, 'r+') as project_file:
            project_yaml = '\n'.join(project_file.readlines())
            env = DataforjEnv('flow.name', env_name)
            yaml_plus_vars = project_yaml \
                .format_map(env.env_config['dataflow-config'])
            flow = dataflow.from_yaml(yaml_plus_vars)
            flow.run(env)


def unit_test(dir: str, env_name: str, step: str):
    project_file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(project_file_name):
        raise Exception(f'There is no Dataforj project in the directory \
                          [{dir}]')
    else:
        with open(project_file_name, 'r+') as project_file:
            project_yaml = '\n'.join(project_file.readlines())
            env = DataforjEnv('flow.name', env_name)
            yaml_plus_vars = project_yaml \
                .format_map(env.env_config['dataflow-config'])
            flow = dataflow.from_yaml(yaml_plus_vars)
            flow.unit_test(env, step)


def debug_step(dir: str, env_name: str, step: str):
    project_file_name = f'{dir}/dataforj.yaml'
    if not os.path.exists(project_file_name):
        raise Exception(f'There is no Dataforj project in the directory \
                          [{dir}]')
    else:
        with open(project_file_name, 'r+') as project_file:
            project_yaml = '\n'.join(project_file.readlines())
            env = DataforjEnv('flow.name', env_name)
            yaml_plus_vars = project_yaml \
                .format_map(env.env_config['dataflow-config'])
            flow = dataflow.from_yaml(yaml_plus_vars)
            flow.debug_step(env, step)


if __name__ == "__main__":
    run('example', 'local')
