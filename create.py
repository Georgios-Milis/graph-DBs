import os
import sys
from dotenv import load_dotenv
from os.path import join as pjoin

import data
from connection import Neo4jConnection, transact_and_time


if __name__ == "__main__":
    # This file path
    path = os.path.dirname(os.path.realpath(__file__))
    # Dataset TODO scale 
    datafile = pjoin(path, 'data', 'scale1_Sloth.txt')


    from dotenv import load_dotenv
    load_dotenv()
    URI = os.getenv('NEO4J_URI')
    USERNAME = os.getenv('NEO4J_USERNAME')
    PASSWORD = os.getenv('NEO4J_PASSWORD')
    INSTANCE = os.getenv('AURA_INSTANCENAME')

    # Initialize connection to database
    connection = Neo4jConnection(URI, USERNAME, PASSWORD, INSTANCE)

    # Durations dictionary
    # durations = {}

    # Load data one at a time, execute transaction and then delete it
    papers = data.get_papers_data(datafile)
    # durations.update(transact_and_time(connection.create_papers, papers))
    # del papers

    authors = data.get_authors_data(datafile)
    # durations.update(transact_and_time(connection.create_authors, authors))
    del authors

    citations = data.get_citations_data(datafile)
    # durations.update(transact_and_time(connection.create_references, citations))
    del citations

    authorships = data.get_authorships_data(datafile)
    # durations.update(transact_and_time(connection.create_authorships, authorships))
    del authorships

    connection.create_paper(papers[0])

    # Close connection
    connection.close()

    # Print so that subprocess.check_output gets the result
    # print(durations)