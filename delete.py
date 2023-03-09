"""
Executes DELETE queries, stores the durations in csv files.
"""
import os
import re
import random
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
DBs = ['janus']#'neo4j', 'janus']


for db in DBs:
    print(db)
    
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
        N_TRIALS = 1
        N_QUERIES = 3
        trials = np.empty((N_TRIALS, N_QUERIES))

        # Node data
        papers, authors, _, _ = data.get_dataset(scale)
        paper_ids = random.choices([paper['id'] for paper in papers], k=N_TRIALS)
        author_ids = random.choices([author['id'] for author in authors], k=N_TRIALS)
        

        for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
            if db == 'janus':
                paper_id = str(paper_id)
                author_id = str(author_id)

            # The following lines have been added for safety purposes.
            # If (and only if), at any time of this loop, the author/paper
            # of the requested id does not exist, then a dummy entry will take its place.
            dummy_paper = {
                'id': paper_id,
                'title': 'Title',
                'year': 2154,
                'n_citation': 0
            }
            dummy_author = {'name': "Name", 'id': author_id, 'org': "Organization"}

            if db == 'neo4j':
                connection.create_paper(dummy_paper)
                connection.create_author(dummy_author)
            else:
                connection.create_paper(*dummy_paper.values())
                connection.create_author(*dummy_author.values())
            
            connection.create_authorship(author_id, paper_id)

            if db == 'neo4j':
                durations.update(transact_and_time(
                    connection.delete_authorship, 
                    (connection.authors_of(paper_id)[0], paper_id)
                ))
            else:
                durations.update(transact_and_time(
                    connection.delete_authorship, 
                    connection.authors_of(paper_id)[0], 
                    paper_id
                ))
            durations.update(transact_and_time(connection.delete_paper, paper_id))
            durations.update(transact_and_time(connection.delete_author, author_id))
            trials[i] = list(durations.values())

        # Aggregate results
        result = np.vstack((
            np.min(trials, axis=0),
            np.max(trials, axis=0),
            np.mean(trials, axis=0)
        ))

        df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
        df.to_csv(pjoin(path, 'results', f'{db}_delete_scale{scale}.csv'))
        print(df)

        # Close connection
        connection.close()
