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

    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    trials = []
    N_TRIALS = 10
    N_QUERIES = 6
    trials = np.empty((N_TRIALS, N_QUERIES))

    # Let's start clean :)
    connection.clear_database()

    for i in range(N_TRIALS):
        # Durations dictionary
        durations = {}
        
        # Load data on database
        durations.update(transact_and_time(connection.create_papers, papers))
        durations.update(transact_and_time(connection.create_authors, authors))
        durations.update(transact_and_time(connection.create_references, citations))
        durations.update(transact_and_time(connection.create_authorships, authorships))
        durations.update({'fill_database': np.sum(list(durations.values()))})
        durations.update(transact_and_time(connection.clear_database))

        trials[i] = list(durations.values())

    result = np.vstack((
        np.min(trials, axis=0),
        np.max(trials, axis=0),
        np.mean(trials, axis=0)
    ))

    df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
    df.to_csv('neo4j_fill_and_empty_scale2.csv')

    print(df)
    
    # Close connection
    connection.close()