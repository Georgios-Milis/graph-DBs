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
    pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[1-2].*\.txt", f)
])

for scale, datafile in enumerate(datafiles, 1):
    # Initialize connection to database
    connection = JanusGraphConnection(URI)

    # Durations dictionary
    durations = {}

    # Measurements
    # TODO milis: run the same #tests finally
    N_TRIALS = 2
    N_QUERIES = 4
    trials = np.empty((N_TRIALS, N_QUERIES))

    # Data
    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    for i in range(N_TRIALS):
        print(f"Trial {i+1}/{N_TRIALS}")

        # TODO atsorvat: what are those dicts and why do we need them?
        dict_papers = {}
        dict_authors = {}
        start_time = time()
        for i, paper in enumerate(papers):
            dict_papers[paper['id']] = i 
            paper['id'] = i
            paper['title'] = paper['title'].replace('\'', '')
            print(connection.create_paper(paper['id'], paper['title'], paper['year'], paper['n_citation']))

        for i, author in enumerate(authors):
            dict_authors[author['id']] = i
            author['id'] = i
            author['name'] = author['name'].replace('\'', '')
            if 'org' in author.keys():
                author['org'] = author['org'].replace('\'', '')
            else:
                author['org'] = ' '
            print(connection.create_author(author['id'], author['name'], author['org']))

        for ref in citations:
            print(ref)
            if ref['from'] in dict_papers.keys() and ref['to'] in dict_papers.keys():
                print(connection.create_reference(dict_papers[ref['from']], dict_papers[ref['to']]))

        for auth in authorships:
            if auth['author'] in dict_authors.keys() and auth['paper'] in dict_papers.keys():
                print(connection.create_authorship(dict_authors[auth['author']], dict_papers[auth['paper']]))

        print(time() - start_time)
