import os
from time import time
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

import create_queries
import read_queries
import update_queries
import delete_queries

class Connection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    create_paper = create_queries.create_paper
    create_author = create_queries.create_author
    create_reference = create_queries.create_reference
    create_authorship = create_queries.create_authorship

    find_paper = read_queries.find_paper
    find_author = read_queries.find_author
    references_of = read_queries.references_of
    references_to = read_queries.references_to
    papers_of = read_queries.papers_of
    authors_of = read_queries.authors_of



if __name__ == "__main__":
    path = os.path.dirname(os.path.realpath(__file__))

    load_dotenv()
    URI = os.getenv('NEO4J_URI')
    USERNAME = os.getenv('NEO4J_USERNAME')
    PASSWORD = os.getenv('NEO4J_PASSWORD')
    INSTANCE = os.getenv('AURA_INSTANCENAME')

    # For local instance
    URI = "bolt://localhost:7687/scale-1"
    USERNAME = "neo4j"
    PASSWORD = "12345678"
    INSTANCE = "scale-1"

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD)

    # Parse dataset
    # people, friendships = parse_mtx(os.path.join(path, 'data', 'socfb-Haverford76.mtx'))

    #print(len(people))
    #print(len(friendships))

    # CREATE
    

    # for friendship in friendships:
    #     p1, p2 = friendship
    #     connection.create_friendship(p1, p2)

    # d = {"id": "101335", "title": "Ontologies in HYDRA - Middleware for Ambient Intelligent Devices.", "year": 2009, "n_citation": 2, "test": 123, "test2": 123}

    data = {
        "Papers": []
    }

    for i in range(50000):
        data["Papers"].append(
            {
                'id': i,
                'title': "Ontologies in HYDRA - Middleware for Ambient Intelligent Devices.",
                'year': 2009,
                'n_citation': 2
            }
        )

    connection.clear_database()
    print("Creating papers...")

    start_time = time()
    
    connection.create_papers(data["Papers"])

    duration = time() - start_time
    print("Duration:", duration)


    # Close
    connection.close()
