"""
Runs all READ-related queries and stores the results in csv files.
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
    LOCAL = False
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
        if re.search("^scale[1-6].*\.txt", f)
    ])

    for scale, datafile in enumerate(datafiles, 1):
        if not LOCAL:
            INSTANCE = os.getenv('AURA_INSTANCENAME')
        else:
            INSTANCE = 'scale-' + str(scale)

        # Initialize connection to database
        connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)
        # Let's start clean :)
        connection.clear_database()

        # Durations dictionary
        durations = {}

        # Fill database
        papers = data.get_papers_data(datafile)
        connection.create_papers(papers)
        del papers
        authors = data.get_authors_data(datafile)
        connection.create_authors(authors)
        del authors
        citations = data.get_citations_data(datafile)
        connection.create_references(citations)
        del citations
        authorships = data.get_authorships_data(datafile)
        connection.create_authorships(authorships)
        del authorships

        N_TRIALS = 10
        N_QUERIES = 4

        # TODO: randomize
        paper_ids = [paper['id'] for paper in papers][:N_TRIALS]
        author_ids = [author['id'] for author in authors][:N_TRIALS]

        trials = np.empty((N_TRIALS, N_QUERIES))

        for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
            durations.update(transact_and_time(connection.title_of_paper, paper_id))
            durations.update(transact_and_time(connection.authors_of, paper_id))
            durations.update(transact_and_time(connection.are_collaborators, author_id, author_id + 42))
            durations.update(transact_and_time(connection.mean_authors_per_paper))

            trials[i] = list(durations.values())

        result = np.vstack((
            np.min(trials, axis=0),
            np.max(trials, axis=0),
            np.mean(trials, axis=0)
        ))

        df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
        df.to_csv(pjoin(path, 'results', f'neo4j_read_scale{scale}.csv'))

        print(df)

        # Close connection
        connection.close()
