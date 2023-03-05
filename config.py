# TODO: add parser to run as CLI
# TODO: merge CRUD.py with CRUD_janus.py


import argparse

# Define perser
parser = argparse.ArgumentParser(description='')

parser.add_argument(
    '--params',
    type = int,
    default = 100,
    help = ''
)


def get_config():
    config = parser.parse_args()
    return config
