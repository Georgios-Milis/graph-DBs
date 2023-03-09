"""
This file contains utility functions that return data attributes ready 
to be inserted into the databases, read from a dataset (text file) 
written by process_DBLP_dataset.py
"""
import os
import re
import json
import pickle
from os.path import join as pjoin


# This file path
path = os.path.dirname(os.path.realpath(__file__))


def get_paper_IDs(datafile):
    """
    Read from dataset and return all paper IDs.
    """
    ids = []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            ids.append(int(json.loads(line)['id']))
    return ids


def get_papers_data(datafile):
    """
    Read from dataset and return paper data.
    """
    papers = []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            # Load data for one paper
            data = json.loads(line)
            # Keep specified fields
            filtered_data = {k: v for k, v in data.items() if k in ['id', 'title', 'year', 'n_citation']}
            # Convert id, year and n_citation to integers
            for k, v in filtered_data.items():
                if k != 'title':
                    filtered_data[k] = int(v)
            papers.append(filtered_data)
    return papers


def get_authors_data(datafile):
    """
    Read from dataset and return unique author data.
    """
    authors = []
    ids = []
    count = 0
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            # Get list of authors for each paper
            data = json.loads(line)['authors']
            count += len(data)
            for author in data:
                # Convert ids to integers
                auth_id = int(author['id'])
                author['id'] = auth_id
                # Extend authors list
                if auth_id not in ids:
                    ids.append(auth_id)
                    authors.append(author)
    return authors


def get_citations_data(datafile):
    """
    Read from dataset and return citation data.
    """
    paper_ids = get_paper_IDs(datafile)
    citations = []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            ref_from = data['id']
            try:
                refs = data['references']
                for ref in refs:
                    ref = int(ref)
                    # Check if paper is in subset
                    if ref in paper_ids:
                        citations.append({
                            "from": int(ref_from),
                            "to": ref
                        })
            except KeyError:
                continue
    return citations


def get_authorships_data(datafile):
    """
    Read from dataset and return authorship data.
    """
    authorships = []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            paper_id = int(data['id'])
            for author in data['authors']:
                # Convert ids to integers
                auth_id = int(author['id'])
                # Convert ids to integers
                authorships.append({
                    "author": auth_id,
                    "paper": paper_id
                })
    return authorships


def get_dataset(scale):
    data = pickle.load(open(pjoin(path, 'data', f'dataset_{scale}.pkl'), 'rb'))
    papers = data['papers']
    authors = data['authors']
    authorships = data['authorships']
    citations = data['citations']
    return papers, authors, authorships, citations


if __name__ == "__main__":
    # Dataset files
    datafiles = sorted([
        pjoin(path, 'data', f) for f in os.listdir(pjoin(path, 'data'))
        if re.search("^scale[1-6].*\.txt", f)
    ])

    for datafile in datafiles:
        # Get scale number
        scale = re.findall('\d+', datafile.split(os.sep)[-1])[0]

        # Data
        papers, authors, authorships, citations = get_dataset(scale)

        # Info
        print("Dataset in scale:", scale)
        print("Papers:", len(papers))
        print("Authors:", len(authors))
        print("Authorships:", len(authorships))
        print("Citations:", len(citations))
        print()
