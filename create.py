import os
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable
from dotenv import load_dotenv


def parse_mtx(path):
    """
    Parse MatrixMarket file and insert into database.
    N, M, K: #rows, #columns, #nonzeros
    """
    with open(path, "r", encoding="utf-8") as f:
        data = f.readlines()
        N = int(data[1].split()[0])
        K = int(data[1].split()[2])
        print(f"Found {N} people with {K} friendships.")

        people = []
        friendships = []
        # Read adjacency data from each line
        for line in data[2:]:
            # Friendships is a list of N two-element lists
            edge = line.split()
            people.extend(edge)
            friendships.append(edge)

        # Keep only unique nodes
        people = set(people)
    return people, friendships


class Connection:
    # TODO: put connection class outside CRUD scripts
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_person(self, person_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_person, person_id)
            # Commenting this out because printing is SLOW!
            # TODO: use tqdm or smth to see progress
            # for row in result:
            #     print(f"Created person: {row['p']}")

    @staticmethod
    def _create_and_return_person(tx, person_id):
        query = (
            "CREATE (p:Person { id: $person_id }) "
            "RETURN p"
        )
        result = tx.run(query, person_id=person_id)
        try:
            return [{"p": row["p"]["id"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    def create_friendship(self, person1_id, person2_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(self._create_and_return_friendship, person1_id, person2_id)
            for row in result:
                print(f"Created friendship between: {row['p1']}, {row['p2']}")

    @staticmethod
    def _create_and_return_friendship(tx, person1_id, person2_id):
        # TODO: maybe handle cases where the people of inserted friendship aren't in the database
        # query = (
        #     "CREATE (p1:Person { id: $person1_id }) "
        #     "CREATE (p2:Person { id: $person2_id }) "
        #     "CREATE (p1)-[:FRIEND]->(p2) "
        #     "RETURN p1, p2"
        # )
        query = (
            """MATCH
                (p1:Person),
                (p2:Person)
            WHERE p1.id = $person1_id AND p2.id = $person2_id
            CREATE (p1)-[r:FRIEND]->(p2)
            RETURN p1, p2
        """
        )
        result = tx.run(query, person1_id=person1_id, person2_id=person2_id)
        try:
            return [{"p1": row["p1"]["id"], "p2": row["p2"]["id"]} for row in result]
        except ServiceUnavailable as exception:
            logging.error(f"{query} raised an error: \n {exception}")
            raise

    def find_person(self, person_id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_person, person_id)
            for row in result:
                print(f"Found person: {row}")

    @staticmethod
    def _find_and_return_person(tx, person_id):
        query = (
            "MATCH (p:Person) "
            "WHERE p.id = $person_id "
            "RETURN p.id AS id"
        )
        result = tx.run(query, person_id=person_id)
        return [row["id"] for row in result]


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
    people, friendships = parse_mtx(os.path.join(path, 'data', 'socfb-Haverford76.mtx'))

    print(len(people), len(friendships))

    # CREATE

    # It is commented out because it takes a long time to run!
    # for person in people:
    #     connection.create_person(person)

    # for friendship in friendships:
    #     p1, p2 = friendship
    #     connection.create_friendship(p1, p2)

    # Test query
    connection.find_person("42")

    # Close
    connection.close()
