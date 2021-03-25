import yaml


class DataforjEnv(object):

    def __init__(self, dataflow_name: str, env: str):
        self.env = env
        self.dataflow_name = dataflow_name
        with open(f'./example/envs/{env}.yaml') as file:
            self.env_config = yaml.load(file, Loader=yaml.FullLoader)

        # conf did not work as expected in Python.  To be invesitgated later.
        # conf = SparkConf() \
        #     .setMaster(env_config['spark']['master']) \
        #     .setAppName(f"Dataforj {dataflow_name} project on {env}")

        # if 'SparkConf' in env_config['spark'].values() and \
        #                isinstance(env_config['spark']['SparkConf'], dict):
        #     for key, value in env_config['spark']['SparkConf']:
        #         pass #conf.set(key, value)

    def spark_session_build(self):
        return f"""
spark = SparkSession \
    .builder \
    .master('{self.env_config['spark']['master']}') \
    .appName(f"Dataforj {self.dataflow_name} project on {self.env}") \
    .getOrCreate()
        """


if __name__ == "__main__":
    DataforjEnv('test', 'local')
