import os
from dotenv import load_dotenv
from connection import JanusGraphConnection
from time import sleep

def run_test(test):
    print(test.__name__, '\u2713' if test() else '\u2717')


def test_create_paper():
    tests = []

    connection.clear_database()
    sleep(2)

    tests.append(connection.find_paper(17) == [])

    connection.create_paper(17, "Title", 2154, 0)
    tests.append(connection.find_paper(17) == ['17'])

    return all(tests)


def test_create_author():
    tests = []

    connection.clear_database()
    sleep(2)

    tests.append(connection.find_author(42) == [])

    connection.create_author(42, "Name", "Organization")
    tests.append(connection.find_author(42) == ['42'])

    return all(tests)



def test_create_reference():
    tests = []
    
    connection.clear_database()
    sleep(2)

    connection.create_paper(1, "Title", 2154, 0)
    connection.create_paper(2, "Title", 2154, 0)
    tests.append(connection.references_of(1) == [])
    tests.append(connection.references_of(2) == [])

    connection.create_reference(1, 2)
    tests.append(connection.references_of(1) == ['2'])
    tests.append(connection.references_of(2) == [])
    tests.append(connection.references_to(1) == [])
    tests.append(connection.references_to(2) == ['1'])

    return all(tests)



def test_create_authorship():
    tests = []
    
    connection.clear_database()
    sleep(2)

    connection.create_author(1, "Name", "Organization")
    connection.create_paper(2, "Title", 2154, 0)
    tests.append(connection.papers_of(1) == [])
    tests.append(connection.authors_of(2) == [])

    connection.create_authorship(1, 2)
    tests.append(connection.papers_of(1) == ['2'])
    tests.append(connection.authors_of(2) == ['1'])

    return all(tests)


if __name__ == "__main__":
    load_dotenv()
    URI = os.getenv('JANUSGRAPH_URI')
    # Initialize connection to database
    connection = JanusGraphConnection(URI)


    tests = [
        test_create_paper,
        test_create_author,
        test_create_reference,
        test_create_authorship,
    ]

    for test in tests: run_test(test)

    connection.close()