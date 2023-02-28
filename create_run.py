"""
Runs all CREATE-related queries and stores the results in csv files.
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

        # Write results in dictionaries
        durations_fill_empty = {}
        durations = {}

        N_TRIALS = 10
        N_QUERIES_fill_empty = 6
        trials_fill_empty = np.empty((N_TRIALS, N_QUERIES_fill_empty))
        N_QUERIES = 4
        trials = np.empty((N_TRIALS, N_QUERIES))

        for i in range(N_TRIALS):
            # Load data one at a time, execute transaction and then delete it
            papers = data.get_papers_data(datafile)
            durations_fill_empty.update(transact_and_time(connection.create_papers, papers))
            del papers

            authors = data.get_authors_data(datafile)
            durations_fill_empty.update(transact_and_time(connection.create_authors, authors))
            del authors

            citations = data.get_citations_data(datafile)
            durations_fill_empty.update(transact_and_time(connection.create_references, citations))
            del citations

            authorships = data.get_authorships_data(datafile)
            durations_fill_empty.update(transact_and_time(connection.create_authorships, authorships))
            del authorships

            # Log those measurements as the time required to fill the database
            durations_fill_empty.update({'fill_database': np.sum(list(durations_fill_empty.values()))})
            

            # Now CREATE on filled database
            dummy_paper = {
                'id': i,
                'title': 'Title',
                'year': 2154,
                'n_citation': 0
            }

            dummy_author = {'name': "Name", 'id': i+1, 'org': "Organization"}

            # Log CREATE durations
            durations.update(transact_and_time(connection.create_paper, dummy_paper))
            durations.update(transact_and_time(connection.create_author, dummy_author))
            durations.update(transact_and_time(connection.create_reference, i, i))
            durations.update(transact_and_time(connection.create_authorship, i, i+1))
            trials[i] = list(durations.values())

            # Clear database and log that duration too
            durations_fill_empty.update(transact_and_time(connection.clear_database))
            trials_fill_empty[i] = list(durations_fill_empty.values())


        # Aggregate CREATE results
        result = np.vstack((
            np.min(trials, axis=0),
            np.max(trials, axis=0),
            np.mean(trials, axis=0)
        ))

        df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
        df.to_csv(pjoin(path, 'results', f'neo4j_create_scale{scale}.csv'))
        print("Create:", df)


        # Aggregate fill_and_empty results
        result_fill_empty = np.vstack((
            np.min(trials_fill_empty, axis=0),
            np.max(trials_fill_empty, axis=0),
            np.mean(trials_fill_empty, axis=0)
        ))

        df = pd.DataFrame(result_fill_empty, columns=durations_fill_empty.keys(), index=['min', 'max', 'mean'])
        df.to_csv(pjoin(path, 'results', f'neo4j_fill_and_empty_scale{scale}.csv'))
        print("Fill and empty:", df)

        # Close connection
        connection.close()
