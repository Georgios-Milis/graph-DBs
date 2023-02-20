import os
import json
import time
from os.path import join as pjoin

from connection import Connection


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
    print("Non-unique authors:", count)
    print("Unique authors:", len(authors))
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

    LOCAL = False

    if not LOCAL:
        from dotenv import load_dotenv
        load_dotenv()
        URI = os.getenv('NEO4J_URI')
        USERNAME = os.getenv('NEO4J_USERNAME')
        PASSWORD = os.getenv('NEO4J_PASSWORD')
        INSTANCE = os.getenv('AURA_INSTANCENAME')
    else:
        URI = "bolt://localhost:7687"
        USERNAME = "neo4j"
        PASSWORD = "12345678"
        INSTANCE = "scale-2"

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)


    # CREATE - start timer
    start_time = time.time()

    # Dataset
    datafile = pjoin(path, 'data', 'documentation.txt')

    # Data
    papers = get_papers_data(datafile)
    print("Papers:", len(papers))
    authors = get_authors_data(datafile)
    citations = get_citations_data(datafile)
    authorships = get_authorships_data(datafile)
    
    paper_ids = [int(paper['id']) for paper in papers]

    print("Citations:", len(citations))
    print("In-data citations:", len([cite for cite in citations if cite['to'] in paper_ids]))
    print("Authorships:", len(authorships))


    # CREATE - stop timer
    duration = time.time() - start_time

    # Print so that subprocess.check_output gets the result
    print(duration)

    # Close
    connection.close()
