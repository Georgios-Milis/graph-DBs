"""
Fills and empties the database, stores the durations in csv files.
"""
import os
import re
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import Neo4jConnection


# This file path
path = os.path.dirname(os.path.realpath(__file__))

# Config
LOCAL = True

load_dotenv()
suffix = 'LOCAL' if LOCAL else 'REMOTE'
URI = os.getenv(f'NEO4J_URI_{suffix}')
USERNAME = os.getenv(f'NEO4J_USERNAME_{suffix}')
PASSWORD = os.getenv(f'NEO4J_PASSWORD_{suffix}')

# Dataset files
datafiles = sorted([
    pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
    if re.search("^scale[1-3].*\.txt", f)
])

def minibatches(list, minibatch_size=1000):
    for i in range(0, len(list), minibatch_size):
        yield list[i:i + minibatch_size]

for scale, datafile in enumerate(datafiles, 1):
    if not LOCAL:
        INSTANCE = os.getenv('NEO4J_INSTANCENAME_REMOTE')
    else:
        INSTANCE = 'scale-' + str(scale)

    # Initialize connection to database
    connection = Neo4jConnection(URI, USERNAME, PASSWORD, INSTANCE)

    # Data
    papers = data.get_papers_data(datafile)
    authors = data.get_authors_data(datafile)
    citations = data.get_citations_data(datafile)
    authorships = data.get_authorships_data(datafile)

    # Let's start clean :)
    connection.clear_database()

    for minibatch in minibatches(papers):
        connection.create_papers(minibatch)

    for minibatch in minibatches(authors):
        connection.create_authors(minibatch)
    
    for minibatch in minibatches(citations):
        connection.create_references(minibatch)

    for minibatch in minibatches(authorships):
        connection.create_authorships(minibatch)

    # Close connection
    connection.close()