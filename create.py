"""
Executes CREATE queries, stores the durations in csv files.
"""
import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import Neo4jConnection, JanusGraphConnection, transact_and_time


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
LOCAL = True
load_dotenv()


# Dataset files
datafiles = sorted([
    pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[1-2].*\.txt", f)
])

# Databases - SUT
DBs = ['neo4j', 'janus']


for db in DBs:
    for scale, datafile in enumerate(datafiles, 1):
        # Connect to the right database
        if db == 'neo4j':
            suffix = 'LOCAL' if LOCAL else 'REMOTE'
            URI = os.getenv(f'NEO4J_URI_{suffix}')
            USERNAME = os.getenv(f'NEO4J_USERNAME_{suffix}')
            PASSWORD = os.getenv(f'NEO4J_PASSWORD_{suffix}')
            if not LOCAL:
                INSTANCE = os.getenv('NEO4J_INSTANCENAME_REMOTE')
            else:
                INSTANCE = 'scale-' + str(scale)
            # Initialize connection to database
            connection = Neo4jConnection(URI, USERNAME, PASSWORD, INSTANCE)
        else:
            URI = os.getenv('JANUSGRAPH_URI')
            INSTANCE = 'scale-' + str(scale)
            # Initialize connection to database
            connection = JanusGraphConnection(URI, INSTANCE)

        # Durations dictionary
        durations = {}

        # Measurements
        N_TRIALS = 10
        N_QUERIES = 4
        trials = np.empty((N_TRIALS, N_QUERIES))

        # Load data one at a time, execute transaction and then delete it
        papers = data.get_papers_data(datafile)
        authors = data.get_authors_data(datafile)
        citations = data.get_citations_data(datafile)
        authorships = data.get_authorships_data(datafile)

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
        df.to_csv(pjoin(path, 'results', f'{db}_create_scale{scale}.csv'))
        print(df)

        # Close connection
        connection.close()
