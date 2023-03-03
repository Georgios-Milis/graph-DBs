"""
Runs all DELETE queries and stores the results in csv files.
"""
import os
import re
import numpy as np
import pandas as pd
from os.path import join as pjoin

import data
from connection import Connection, transact_and_time


if __name__ == "__main__":
    # This file path
    path = os.path.dirname(os.path.realpath(__file__))

    # Config
    LOCAL = True
    if not LOCAL:
        from dotenv import load_dotenv
        load_dotenv()
        URI = os.getenv('NEO4J_URI')
        USERNAME = os.getenv('NEO4J_USERNAME')
        PASSWORD = os.getenv('NEO4J_PASSWORD')
    else:
        URI = "bolt://localhost:7687"
        USERNAME = "neo4j"
        PASSWORD = "12345678"


    # Dataset files
    datafiles = sorted([
        pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
        if re.search("^scale[1-4].*\.txt", f)
    ])

    for scale, datafile in enumerate(datafiles, 1):
        if not LOCAL:
            INSTANCE = os.getenv('AURA_INSTANCENAME')
        else:
            INSTANCE = 'scale-' + str(scale)

        # Initialize connection to database
        connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)
        # # Let's start clean :)
        # connection.clear_database()

        # Durations dictionary
        durations = {}

        # Measurements
        N_TRIALS = 10
        N_QUERIES = 3
        trials = np.empty((N_TRIALS, N_QUERIES))

        # Node data
        papers = data.get_papers_data(datafile)
        authors = data.get_authors_data(datafile)

        # TODO: randomize
        papers = papers[:N_TRIALS]
        authors = authors[:N_TRIALS]
        
        connection.create_papers(papers)
        connection.create_authors(authors)
        
        paper_ids = [paper['id'] for paper in papers]
        author_ids = [author['id'] for author in authors]

        for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
            connection.create_authorship(author_id, paper_id)
            durations.update(transact_and_time(
                connection.delete_authorship, 
                (connection.authors_of(paper_id)[0], paper_id)
            ))
            durations.update(transact_and_time(connection.delete_paper, paper_id))
            durations.update(transact_and_time(connection.delete_author, author_id))

            trials[i] = list(durations.values())

        result = np.vstack((
            np.min(trials, axis=0),
            np.max(trials, axis=0),
            np.mean(trials, axis=0)
        ))

        df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
        df.to_csv(pjoin(path, 'results', f'neo4j_delete_scale{scale}.csv'))

        print(df)

        # Close connection
        connection.close()
