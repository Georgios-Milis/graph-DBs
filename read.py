import os
import random
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import Neo4jConnection, JanusGraphConnection, transact_and_time


# Config
START = 1
END = 4
LOCAL = True
load_dotenv()


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Databases - SUT
DBs = ['neo4j', 'janus']

DBs = ['janus']


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
            connection.clear_database()
            connection.load_graph(scale)
            

        # Durations dictionary
        durations = {}

        # Measurements
        N_TRIALS = 10
        N_QUERIES = 3
        trials = np.empty((N_TRIALS, N_QUERIES))

        # Node data
        papers, authors, _, _ = data.get_dataset(scale)
        paper_ids = random.choices([paper['id'] for paper in papers], k=N_TRIALS)
        author_ids = random.choices([author['id'] for author in authors], k=N_TRIALS)
        

        for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
            if db == 'neo4j':
                next_author_id = author_id + 42
            else:
                paper_id = str(paper_id)
                next_author_id = str(author_id + 42)
                author_id = str(author_id)
        
            durations.update(transact_and_time(connection.title_of_paper, paper_id))
            durations.update(transact_and_time(connection.authors_of, paper_id))
            durations.update(transact_and_time(connection.are_collaborators, author_id, next_author_id))
            #durations.update(transact_and_time(connection.mean_authors_per_paper))
            trials[i] = list(durations.values())

        # Aggregate results
        result = np.vstack((
            np.min(trials, axis=0),
            np.max(trials, axis=0),
            np.mean(trials, axis=0)
        ))

        df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
        df.to_csv(pjoin(path, 'results', f'{db}_read_scale{scale}.csv'))
        print(df)

        # Close connection
        connection.close()
