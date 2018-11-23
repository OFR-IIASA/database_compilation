import yaml
import os
import sys


def load_config(config_file=False):
    if not config_file:
        print('Loading default configuration file')
        config_file = os.path.join('config', 'config.yaml')
    else:
        print('Loading configuration file', config_file)
    if not os.path.exists(config_file):
        print('Cannot find', config_file)
        sys.exit(1)
    with open(config_file, 'r') as stream:
        config = yaml.load(stream)
    return(config)
