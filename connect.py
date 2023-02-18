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

    delete_paper = delete_queries.delete_paper
    delete_author = delete_queries.delete_author
    delete_reference = delete_queries.delete_reference
    delete_authorship = delete_queries.delete_authorship

if __name__ == "__main__":
    path = os.path.dirname(os.path.realpath(__file__))

    load_dotenv()
    URI = os.getenv('NEO4J_URI')
    USERNAME = os.getenv('NEO4J_USERNAME')
    PASSWORD = os.getenv('NEO4J_PASSWORD')
    INSTANCE = os.getenv('AURA_INSTANCENAME')

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD)

    # Parse dataset
    # people, friendships = parse_mtx(os.path.join(path, 'data', 'socfb-Haverford76.mtx'))

    #print(len(people))
    #print(len(friendships))

    # CREATE
    start_time = time()
    # It is commented out because it takes a long time to run!
    # for person in people:
    #     connection.create_person(person)

    # for friendship in friendships:
    #     p1, p2 = friendship
    #     connection.create_friendship(p1, p2)

    # Test query
    # result = connection.find_person("32")
    # print(result)

    #connection.delete_person('1')
    #connection.delete_person('76')

    #connection.delete_relationship(('76', '1'))
    #connection.delete_relationship(('1', '76'), False)

    #connection.neighbours('76', 'all')

    # d = {"id": "101335", "title": "Ontologies in HYDRA - Middleware for Ambient Intelligent Devices.", "year": 2009, "n_citation": 2, "test": 123, "test2": 123}


    # connection.create_node
    # connection.delete_node("Asdf$attributes", '180')
    #connection.create_person('76')
    #connection.create_friendship('76', '1')

    duration = time() - start_time

    # Print so that subprocess.check_output gets the result
    print(duration)


    # Close
    connection.close()