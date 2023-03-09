"""
Fills and empties the database, stores the durations in csv files.
"""
import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

import data
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
    if re.search("^scale[1-6].*\.txt", f)
])

def minibatches(list, minibatch_size=1000):
    for i in range(0, len(list), minibatch_size):
        yield list[i:i + minibatch_size]


for scale, datafile in enumerate(datafiles, 1):
    if not LOCAL:
        INSTANCE = os.getenv('NEO4J_INSTANCENAME_REMOTE')
    else:
        INSTANCE = 'scale-' + str(scale)

    # Initialize connection to database
    connection = Neo4jConnection(URI, USERNAME, PASSWORD, INSTANCE)

    # Write results in a dictionary
    durations = {}

    # Measurements
    N_TRIALS = 10
    if scale == 5:
        N_TRIALS = 5
    elif scale > 5:
        N_TRIALS = 3
    N_QUERIES = 6
    trials = [[] for _ in range(N_TRIALS)]

    # Data
    papers, authors, authorships, citations = data.get_dataset(scale)


    for i in range(N_TRIALS):
        print(f"Trial {i+1}/{N_TRIALS}")

        durations.update(transact_and_time(connection.clear_database))

        connection.remove_constraints()
        connection.paper_constraints()
        connection.author_constraints()

        transaction_time = 0
        for minibatch in minibatches(papers):
            transaction_time += transact_and_time(connection.create_papers, minibatch)['create_papers']
        durations.update({'create_papers': transaction_time})

        transaction_time = 0
        for minibatch in minibatches(authors):
            transaction_time += transact_and_time(connection.create_authors, minibatch)['create_authors']
        durations.update({'create_authors': transaction_time})

        transaction_time = 0
        for minibatch in minibatches(citations):
            transaction_time += transact_and_time(connection.create_references, minibatch)['create_references']
        durations.update({'create_references': transaction_time})

        transaction_time = 0
        for minibatch in minibatches(authorships):
            transaction_time += transact_and_time(connection.create_authorships, minibatch)['create_authorships']
        durations.update({'create_authorships': transaction_time})

        # Log those measurements as the time required to fill the database
        durations.update({'fill_database': np.sum(list(durations.values()))})

        trials[i] = list(durations.values())
        
    
    # Ignore 1st clear because we don't have a full database
    trials[0][0] = np.nan
            
    # Aggregate fill_and_empty results
    trials = np.array(trials)
    result = np.vstack((
        np.nanmin(trials, axis=0),
        np.nanmax(trials, axis=0),
        np.nanmean(trials, axis=0)
    ))

    df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
    df.to_csv(pjoin(path, 'results', f'neo4j_fill_empty_scale{scale}.csv'))
    print(df)

    # Close connection
    connection.close()
