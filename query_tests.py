import os
from dotenv import load_dotenv
from os.path import join as pjoin
from connection import Connection


def run_test(test):
    print(test.__name__, '\u2713' if test() else '\u2717')


def test_create_paper():
    tests = []

    connection.clear_database()
    tests.append(connection.find_paper(17) == [])

    connection.create_paper({'id': 17, 'title': "Title", 'year': 2154, 'n_citation': 0})
    tests.append(connection.find_paper(17) == [17])

    return all(tests)


def test_create_author():
    tests = []

    connection.clear_database()
    tests.append(connection.find_author(42) == [])

    connection.create_author({'name': "Name", 'id': 42, 'org': "Organization"})
    tests.append(connection.find_author(42) == [42])

    return all(tests)


def test_create_reference():
    tests = []
    
    connection.clear_database()
    connection.create_paper({'id': 1, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_paper({'id': 2, 'title': "Title", 'year': 2154, 'n_citation': 0})
    tests.append(connection.references_of(1) == [])
    tests.append(connection.references_of(2) == [])

    connection.create_reference(1, 2)
    tests.append(connection.references_of(1) == [2])
    tests.append(connection.references_of(2) == [])
    tests.append(connection.references_to(1) == [])
    tests.append(connection.references_to(2) == [1])

    return all(tests)


def test_create_authorship():
    tests = []
    
    connection.clear_database()
    connection.create_author({'name': "Name", 'id': 1, 'org': "Organization"})
    connection.create_paper({'id': 2, 'title': "Title", 'year': 2154, 'n_citation': 0})
    tests.append(connection.papers_of(1) == [])
    tests.append(connection.authors_of(2) == [])

    connection.create_authorship(1, 2)
    tests.append(connection.papers_of(1) == [2])
    tests.append(connection.authors_of(2) == [1])

    return all(tests)


def test_coauthorship():
    tests = []
    
    connection.clear_database()
    connection.create_author({'name': "Name", 'id': 1, 'org': "Organization"})
    connection.create_author({'name': "Name", 'id': 2, 'org': "Organization"})
    connection.create_paper({'id': 3, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_authorship(1, 3)

    tests.append(connection.are_collaborators(1, 2) == [])

    connection.create_authorship(2, 3)
    tests.append(connection.are_collaborators(1, 2) == [3])

    return all(tests)


def test_mean_authors_per_paper():
    tests = []
    
    connection.clear_database()
    connection.create_author({'name': "Name", 'id': 1, 'org': "Organization"})
    connection.create_author({'name': "Name", 'id': 2, 'org': "Organization"})
    connection.create_author({'name': "Name", 'id': 3, 'org': "Organization"})
    connection.create_author({'name': "Name", 'id': 4, 'org': "Organization"})

    connection.create_paper({'id': 1, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_paper({'id': 2, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_paper({'id': 3, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_paper({'id': 4, 'title': "Title", 'year': 2154, 'n_citation': 0})

    connection.create_authorship(1, 1)
    connection.create_authorship(2, 1)
    connection.create_authorship(3, 1)
    connection.create_authorship(4, 1)
    tests.append(connection.mean_authors_per_paper() == 1)

    connection.create_authorship(1, 2)
    connection.create_authorship(2, 2)
    connection.create_authorship(3, 2)
    tests.append(connection.mean_authors_per_paper() == 1.75)

    connection.create_authorship(1, 3)
    connection.create_authorship(2, 3)
    tests.append(connection.mean_authors_per_paper() == 2.25)

    connection.create_authorship(1, 4)
    tests.append(connection.mean_authors_per_paper() == 2.5)

    return all(tests)


def test_delete_paper():
    tests = []

    connection.clear_database()
    connection.create_paper({'id': 17, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.delete_paper(17)
    tests.append(connection.find_paper(17) == [])

    return all(tests)


def test_delete_author():
    tests = []

    connection.clear_database()
    connection.create_author({'name': "Name", 'id': 42, 'org': "Organization"})
    connection.delete_author(42)
    tests.append(connection.find_author(42) == [])

    return all(tests)


def test_delete_reference():
    tests = []
    
    connection.clear_database()
    connection.create_paper({'id': 1, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_paper({'id': 2, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_reference(1, 2)
    connection.delete_reference((1, 2))
    tests.append(connection.references_of(1) == [])
    tests.append(connection.references_of(2) == [])
    tests.append(connection.references_to(1) == [])
    tests.append(connection.references_to(2) == [])

    return all(tests)


def test_create_authorship():
    tests = []
    
    connection.clear_database()
    connection.create_author({'name': "Name", 'id': 1, 'org': "Organization"})
    connection.create_paper({'id': 2, 'title': "Title", 'year': 2154, 'n_citation': 0})
    connection.create_authorship(1, 2)
    connection.delete_authorship((1, 2))
    tests.append(connection.papers_of(1) == [])
    tests.append(connection.authors_of(2) == [])

    return all(tests)


def test_rename_paper():
    tests = []
    
    connection.clear_database()
    connection.create_paper({'id': 1, 'title': "Title", 'year': 2154, 'n_citation': 0})
    tests.append(connection.title_of_paper(1) == ["Title"])

    connection.rename_paper(1, "New Title")
    tests.append(connection.title_of_paper(1) == ["New Title"])

    return all(tests)


def test_change_org():
    tests = []
    
    connection.clear_database()
    connection.create_author({'name': "Name", 'id': 1, 'org': "Organization"})
    tests.append(connection.org_of_author(1) == ["Organization"])

    connection.change_org(1, "New Organization")
    tests.append(connection.org_of_author(1) == ["New Organization"])
    connection.org_of_author(1)

    return all(tests)


if __name__ == "__main__":
    # This file path
    path = os.path.dirname(os.path.realpath(__file__))
    # Dataset TODO scale 
    datafile = pjoin(path, 'data', 'Magnetism.txt')

    from dotenv import load_dotenv
    load_dotenv()
    URI = os.getenv('NEO4J_URI')
    USERNAME = os.getenv('NEO4J_USERNAME')
    PASSWORD = os.getenv('NEO4J_PASSWORD')
    INSTANCE = os.getenv('AURA_INSTANCENAME')

    # Initialize connection to database
    connection = Connection(URI, USERNAME, PASSWORD, INSTANCE)

    tests = [
        test_create_paper,
        test_create_author,
        test_create_reference,
        test_create_authorship,
        test_coauthorship,
        test_mean_authors_per_paper,
        test_delete_paper,
        test_delete_author,
        test_delete_reference,
        test_create_authorship,
        test_rename_paper,
        test_change_org,
    ]

    for test in tests: run_test(test)

    connection.close()