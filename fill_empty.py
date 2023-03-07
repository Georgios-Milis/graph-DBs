"""
Fills and empties the database, stores the durations in csv files.
"""
import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin
import collections, functools, operator

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
    if re.search("^scale[1-4].*\.txt", f)
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
    # TODO milis: run the same #tests finally
    N_TRIALS = 2
    N_QUERIES = 6
    trials = np.empty((N_TRIALS, N_QUERIES))

    # Data
    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    for i in range(N_TRIALS):
        print(f"Trial {i+1}/{N_TRIALS}")
        # Let's start clean :)
        durations.update(transact_and_time(connection.clear_database))
        connection.remove_constraints()

        # Load data one at a time, execute transaction and then delete it
        connection.author_constraints()
        connection.paper_constraints()

        durations.update(dict(functools.reduce(operator.add, map(
            collections.Counter, 
            [transact_and_time(connection.create_papers, minibatch) for minibatch in minibatches(papers)]
        ))))

        durations.update(dict(functools.reduce(operator.add, map(
            collections.Counter, 
            [transact_and_time(connection.create_authors, minibatch) for minibatch in minibatches(authors)]
        ))))

        durations.update(dict(functools.reduce(operator.add, map(
            collections.Counter, 
            [transact_and_time(connection.create_references, minibatch) for minibatch in minibatches(citations)]
        ))))

        durations.update(dict(functools.reduce(operator.add, map(
            collections.Counter, 
            [transact_and_time(connection.create_authorships, minibatch) for minibatch in minibatches(authorships)]
        ))))

        # Log those measurements as the time required to fill the database
        durations.update({'fill_database': np.sum(list(durations.values()))})
        trials[i] = list(durations.values())


    # Aggregate fill_and_empty results
    result = np.vstack((
        np.min(trials, axis=0),
        np.max(trials, axis=0),
        np.mean(trials, axis=0)
    ))

    df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
    df.to_csv(pjoin(path, 'results', f'neo4j_fill_empty_scale{scale}.csv'))
    print(df)

    # Close connection
    connection.close()


    #TODO milis: ignore 1st clear