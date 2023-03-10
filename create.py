"""
Executes CREATE queries, stores the durations in csv files.
"""
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

from connection import Neo4jConnection, JanusGraphConnection, transact_and_time


# Config
START = 1
END = 6
LOCAL = True
load_dotenv()


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Databases - SUT
DBs = ['neo4j', 'janus']

DBs = ['neo4j']


for db in DBs:
    print(db)
    
    for scale in range(START, END + 1):
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


        for t in range(N_TRIALS):
            paper_id = 0
            author_id = 0

            while connection.find_paper(paper_id) != []:
                paper_id = np.random.randint(0, 9999999)
            while connection.find_author(author_id) != []:
                author_id = np.random.randint(0, 9999999)

            if db == 'janus':
                paper_id = str(paper_id)
                author_id = str(author_id)

            dummy_paper = {
                'id': paper_id,
                'title': 'Title',
                'year': 2154,
                'n_citation': 0
            }
            dummy_author = {'name': "Name", 'id': author_id, 'org': "Organization"}

            # Log CREATE durations
            if db == 'neo4j':
                durations.update(transact_and_time(connection.create_paper, dummy_paper))
                durations.update(transact_and_time(connection.create_author, dummy_author))
            else:
                durations.update(transact_and_time(connection.create_paper, *dummy_paper.values()))
                durations.update(transact_and_time(connection.create_author, *dummy_author.values()))

            durations.update(transact_and_time(connection.create_reference, paper_id, paper_id))
            durations.update(transact_and_time(connection.create_authorship, author_id, paper_id))
            trials[t] = list(durations.values())

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
