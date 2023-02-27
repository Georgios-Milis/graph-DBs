import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import Connection, transact_and_time



if __name__ == "__main__":

    # This file path
    path = os.path.dirname(os.path.realpath(__file__))
    # Dataset TODO scale 
    datafile = pjoin(path, 'data', 'Magnetism.txt')

    from dotenv import load_dotenv
    load_dotenv()
    URI = os.getenv('NEO4J_URI')
    USERNAME = os.getenv('NEO4J_USERNAME')
    PASSWORD = os.getenv('NEO4J_PASSWORD')
    INSTANCE = os.getenv('AURA_INSTANCENAME')

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)

    # Durations dictionary
    durations = {}

    connection.clear_database()

    # Load data one at a time, execute transaction and then delete it
    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    connection.create_papers(papers)
    connection.create_authors(authors)
    connection.create_references(citations)
    connection.create_authorships(authorships)

    N_TRIALS = 10
    N_QUERIES = 4

    connection.create_author({'name': "Name", 'id': 0, 'org': "Organization"})

    trials = np.empty((N_TRIALS, N_QUERIES))

    for i in range(N_TRIALS):
        durations = {}

        dummy_paper = {
            'id': i,
            'title': 'Title',
            'year': 2154,
            'n_citation': 0
        }

        dummy_author = {'name': "Name", 'id': i + 1, 'org': "Organization"}

        durations.update(transact_and_time(connection.create_paper, dummy_paper))
        durations.update(transact_and_time(connection.create_author, dummy_author))
        durations.update(transact_and_time(connection.create_reference, i, i))
        durations.update(transact_and_time(connection.create_authorship, i, i + 1))

        trials[i] = list(durations.values())

    result = np.vstack((
        np.min(trials, axis=0),
        np.max(trials, axis=0),
        np.mean(trials, axis=0)
    ))

    df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
    df.to_csv('results/neo4j_create_scale2.csv')

    print(df)

    # Close connection
    connection.close()

    # Print so that subprocess.check_output gets the result
    print(durations)