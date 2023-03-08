import os
import re
import numpy as np
import pandas as pd
from time import time
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import JanusGraphConnection, transact_and_time


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
load_dotenv()
URI = os.getenv('JANUSGRAPH_URI')


# Dataset files
datafiles = sorted([
    pjoin(path, '..', 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[2].*\.txt", f)
])

for scale, datafile in enumerate(datafiles, 1):
    INSTANCE = 'scale-' + str(scale)
    # Initialize connection to database
    connection = JanusGraphConnection(URI, instance=INSTANCE)

    # Durations dictionary 
    durations = {}

    # Measurements
    # TODO milis: run the same #tests finally
    N_TRIALS = 1
    N_QUERIES = 4
    trials = np.empty((N_TRIALS, N_QUERIES))

    # Data
    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    for i in range(N_TRIALS):
        print(f"Trial {i+1}/{N_TRIALS}")

        start_time = time()
        for i, paper in enumerate(papers): 
            paper['id'] = str(paper['id'])
            paper['title'] = paper['title'].replace('\'', '')
            print(connection.create_paper(paper['id'], paper['title'], paper['year'], paper['n_citation']))

        for i, author in enumerate(authors):
            author['id'] = str(author['id'])
            author['name'] = author['name'].replace('\'', '')
            if 'org' in author.keys():
                author['org'] = author['org'].replace('\'', '')
            else:
                author['org'] = ' '
            print(connection.create_author(author['id'], author['name'], author['org']))

        for ref in citations:
            ref['from'] = str(ref['from'])
            ref['to'] = str(ref['to'])
            print(connection.create_reference(ref['from'], ref['to']))

        for auth in authorships:
            auth['auhtor'] = str(auth['author'])
            auth['paper'] = str(auth['paper'])
            print(connection.create_authorship(auth['author'], auth['paper']))

        print(time() - start_time)
