import os
import random
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import JanusGraphConnection, transact_and_time


scale = 4


# Config
load_dotenv()
path = os.path.dirname(os.path.realpath(__file__))
URI = os.getenv('JANUSGRAPH_URI')

# Initialize connection to database
connection = JanusGraphConnection(URI, scale)

# print(transact_and_time(connection.load_graph))

# CREATE ======================================================================
# Durations dictionary
durations = {}

# Measurements
N_TRIALS = 10
N_QUERIES = 4
trials = np.empty((N_TRIALS, N_QUERIES))


for t in range(N_TRIALS):
    paper_id = 0
    author_id = 0

    paper_id = str(paper_id)
    author_id = str(author_id)

    while connection.find_paper(paper_id) != []:
        paper_id = np.random.randint(0, 9999999)
        paper_id = str(paper_id)
    while connection.find_author(author_id) != []:
        author_id = np.random.randint(0, 9999999)
        author_id = str(author_id)


    dummy_paper = {
        'id': paper_id,
        'title': 'Title',
        'year': 2154,
        'n_citation': 0
    }
    dummy_author = {'id': author_id, 'name': "Name", 'org': "Organization"}

    # Log CREATE durations
    durations.update(transact_and_time(connection.create_paper, *dummy_paper.values()))
    durations.update(transact_and_time(connection.create_author, *dummy_author.values()))

    durations.update(transact_and_time(connection.create_reference, paper_id, paper_id))
    durations.update(transact_and_time(connection.create_authorship, author_id, paper_id))
    trials[t] = list(durations.values())

# Aggregate results
result = np.vstack((
    np.min(trials, axis=0),
    np.max(trials, axis=0),
    np.mean(trials, axis=0)
))

df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
df.to_csv(pjoin(path, 'results', f'janus_create_scale{scale}.csv'))
print(df)


# READ ========================================================================
# Durations dictionary
durations = {}

# Measurements
N_TRIALS = 10
N_QUERIES = 4
trials = np.empty((N_TRIALS, N_QUERIES))

# Node data
papers, authors, _, _ = data.get_dataset(scale)
paper_ids = random.choices([paper['id'] for paper in papers], k=N_TRIALS)
author_ids = random.choices([author['id'] for author in authors], k=N_TRIALS)


for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
    paper_id = str(paper_id)
    next_author_id = str(author_id + 42)
    author_id = str(author_id)

    durations.update(transact_and_time(connection.title_of_paper, paper_id))
    durations.update(transact_and_time(connection.authors_of, paper_id))
    durations.update(transact_and_time(connection.are_collaborators, author_id, next_author_id))
    durations.update(transact_and_time(connection.mean_authors_per_paper))
    trials[i] = list(durations.values())

# Aggregate results
result = np.vstack((
    np.min(trials, axis=0),
    np.max(trials, axis=0),
    np.mean(trials, axis=0)
))

df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
df.to_csv(pjoin(path, 'results', f'janus_read_scale{scale}.csv'))
print(df)


# UPDATE ======================================================================
# Durations dictionary
durations = {}

# Measurements
N_TRIALS = 10
N_QUERIES = 2
trials = np.empty((N_TRIALS, N_QUERIES))

# Node data
papers, authors, _, _ = data.get_dataset(scale)
paper_ids = random.choices([paper['id'] for paper in papers], k=N_TRIALS)
author_ids = random.choices([author['id'] for author in authors], k=N_TRIALS)


for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
    paper_id = str(paper_id)
    author_id = str(author_id)
    
    durations.update(transact_and_time(connection.rename_paper, paper_id, "New Title"))
    durations.update(transact_and_time(connection.change_org, author_id, "New Organization"))
    trials[i] = list(durations.values())

# Aggregate results
result = np.vstack((
    np.min(trials, axis=0),
    np.max(trials, axis=0),
    np.mean(trials, axis=0)
))

df = pd.DataFrame(result, columns=durations.keys(), index=['min', 'max', 'mean'])
df.to_csv(pjoin(path, 'results', f'janus_update_scale{scale}.csv'))
print(df)


# DELETE ======================================================================
# Durations dictionary
durations = {}

# Measurements
N_TRIALS = 10
N_QUERIES = 3
trials = np.empty((N_TRIALS, N_QUERIES))

# Node data
papers, authors, _, _ = data.get_dataset(scale)
paper_ids = random.choices([paper['id'] for paper in papers], k=N_TRIALS)
author_ids = random.choices([author['id'] for author in authors], k=N_TRIALS)


for i, (paper_id, author_id) in enumerate(zip(paper_ids, author_ids)):
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
    dummy_author = {'id': author_id, 'name': "Name", 'org': "Organization"}

    connection.create_paper(*dummy_paper.values())
    connection.create_author(*dummy_author.values())
    
    connection.create_authorship(author_id, paper_id)

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
df.to_csv(pjoin(path, 'results', f'janus_delete_scale{scale}.csv'))
print(df)

# Close connection
connection.close()
