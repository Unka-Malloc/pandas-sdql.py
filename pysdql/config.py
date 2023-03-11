import os
import pathlib
from importlib.util import find_spec

import toml


def get_pysdql_path():
    return pathlib.Path(os.path.abspath(os.path.dirname(__file__))).absolute()

def is_pandas_available():
    if find_spec("pandas") is not None:
        return True

    return False

def is_duckdb_available():
    if find_spec("duckdb") is not None:
        return True

    return False

def set_verify(val: bool):
    with open(get_config_path(), 'r') as file:
        config = toml.load(file)
        config['configuration']['enable_verification'] = val

    with open(get_config_path(), 'w') as file:
        toml.dump(config, file)

def is_verification_enabled():
    with open(get_config_path(), 'r') as file:
        return toml.load(file)['configuration']['enable_verification']
    
def get_datapath():
    with open(get_config_path(), 'r') as file:
        return toml.load(file)['configuration']['data_path']

def get_config_path():
    return f'{get_pysdql_path()}/config.toml'

def get_config():
    return toml.load(f'{get_pysdql_path()}/config.toml')
