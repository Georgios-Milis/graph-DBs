"""This script can fill JanusGraph, sending data one-by-one."""
import os
import numpy as np
import pandas as pd
from time import time
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import JanusGraphConnection, transact_and_time


# Config
START = 3
END = 4
load_dotenv()

URI = os.getenv('JANUSGRAPH_URI')


# This file path
path = os.path.dirname(os.path.realpath(__file__))

for scale in range(START, END + 1):
    INSTANCE = 'scale-' + str(scale)
    # Initialize connection to database
    connection = JanusGraphConnection(URI, instance=INSTANCE)

    # Durations dictionary 
    durations = {}

    # Measurements
    N_TRIALS = 1
    N_QUERIES = 2
    trials = [[] for _ in range(N_TRIALS)]

    # Data
    papers, authors, authorships, citations = data.get_dataset(scale)

    for t in range(N_TRIALS):
        print(f"Trial {t+1}/{N_TRIALS}")

        durations.update(transact_and_time(connection.clear_database))

        start_time = time()
        for i, paper in enumerate(papers): 
            paper['id'] = str(paper['id'])
            #paper['id'] = str(i)
            paper['title'] = paper['title'].replace('\'', '')
            connection.create_paper(paper['id'], paper['title'], paper['year'], paper['n_citation'])
        print("Papers inserted.")

        for i, author in enumerate(authors):
            author['id'] = str(author['id'])
            #author['id'] = str(i)
            author['name'] = author['name'].replace('\'', '')
            if 'org' in author.keys():
                author['org'] = author['org'].replace('\'', '')
            else:
                author['org'] = ' '
            connection.create_author(author['id'], author['name'], author['org'])
        print("Authors inserted.")

        for ref in citations:
            ref['from'] = str(ref['from'])
            ref['to'] = str(ref['to'])
            connection.create_reference(ref['from'], ref['to'])
        print("Citations inserted.")

        for auth in authorships:
            auth['author'] = str(auth['author'])
            auth['paper'] = str(auth['paper'])
            connection.create_authorship(auth['author'], auth['paper'])
        print("Authorships inserted.")

        durations['fill_database'] = time() - start_time

        trials[t] = list(durations.values())
        
    
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
    df.to_csv(pjoin(path, 'results', f'janus_fill_empty_scale{scale}.csv'))
    print(df)

    # Close connection
    connection.close()
