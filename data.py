"""
This file contains utility functions that return data attributes ready 
to be inserted into the databases, read from a dataset (text file) 
written by process_DBLP_dataset.py
"""
import os
import json
from os.path import join as pjoin


def get_paper_IDs(datafile):
    """
    Read from dataset and return all paper IDs.
    """
    ids = []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            ids.append(int(json.loads(line)))
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
    citations = []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            data = json.loads(line)
            ref_from = data['id']
            try:
                refs = data['references']
                for ref in refs:
                    # Convert ids to integers
                    citations.append({
                        "from": int(ref_from),
                        "to": int(ref)
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


if __name__ == "__main__":
    path = os.path.dirname(os.path.realpath(__file__))

    # Dataset
    datafile = pjoin(path, 'data', 'Aeroacoustics.txt')

    # Data info
    papers = get_papers_data(datafile)
    print("Papers:", len(papers))

    authors = get_authors_data(datafile)
    print("Authors:", len(authors))

    authorships = get_authorships_data(datafile)
    print("Authorships:", len(authorships))
    
    citations = get_citations_data(datafile)
    print("Citations:", len(citations))

    paper_ids = [int(paper['id']) for paper in papers]
    print("In-data citations:", len([cite for cite in citations if cite['to'] in paper_ids]))
