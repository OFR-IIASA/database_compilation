import sqlalchemy
from load_config import load_config


def database_connect(config_file=False):
    config = load_config(config_file)
    return(sqlalchemy.create_engine(
        'postgresql://{}:{}@localhost:{}/{}'
        .format(
                config['database_config']['database_user'],
                config['database_config']['database_password'],
                config['database_config']['database_port'],
                config['database_config']['database_name'])))
