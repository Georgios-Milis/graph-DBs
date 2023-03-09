"""
Executes CREATE queries, stores the durations in csv files.
"""
import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

from connection import Neo4jConnection, transact_and_time


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
LOCAL = True

load_dotenv()
suffix = 'LOCAL' if LOCAL else 'REMOTE'
URI = os.getenv(f'NEO4J_URI_{suffix}')
USERNAME = os.getenv(f'NEO4J_USERNAME_{suffix}')
PASSWORD = os.getenv(f'NEO4J_PASSWORD_{suffix}')


# Dataset files
datafiles = sorted([
    pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[1-4].*\.txt", f)
])

for scale, datafile in enumerate(datafiles, 1):
    if not LOCAL:
        INSTANCE = os.getenv('NEO4J_INSTANCENAME_REMOTE')
    else:
        INSTANCE = 'scale-' + str(scale)

    # Initialize connection to database
    connection = Neo4jConnection(URI, USERNAME, PASSWORD, INSTANCE)

    # Durations dictionary
    durations = {}

    # Measurements
    N_TRIALS = 10
    N_QUERIES = 4
    trials = np.empty((N_TRIALS, N_QUERIES))


    for i in range(N_TRIALS):
        dummy_paper = {
            'id': i,
            'title': 'Title',
            'year': 2154,
            'n_citation': 0
        }
        dummy_author = {'name': "Name", 'id': i + 1, 'org': "Organization"}

        # Log CREATE durations
        durations.update(transact_and_time(connection.create_paper, dummy_paper))
        durations.update(transact_and_time(connection.create_author, dummy_author))
        durations.update(transact_and_time(connection.create_reference, i, i))
        durations.update(transact_and_time(connection.create_authorship, i, i + 1))
        trials[i] = list(durations.values())

    # Aggregate results
    result = np.vstack((
        np.min(trials, axis=0),
        np.max(trials, axis=0),
        np.mean(trials, axis=0)
    ))

    df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
    df.to_csv(pjoin(path, 'results', f'neo4j_create_scale{scale}.csv'))
    print(df)

    # Close connection
    connection.close()
