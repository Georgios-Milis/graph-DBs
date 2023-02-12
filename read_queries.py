def neigbours(self, id, direction='all'):
    """
    Return neighbours of a node (incoming, outcoming or both).
    """
    assert direction == 'in' or direction == 'out' or direction == 'all'

    def delete_relationship_tx(tx, id, direction):
        if direction == 'in':
            query = (
                "MATCH (p1:Person)-[r]->(p2:Person) "
                "WHERE p2.id = $id "
                "RETURN DISTINCT p1.id AS id"
            )
        elif direction == 'out':
            query = (
                "MATCH (p1:Person)-[r]->(p2:Person) "
                "WHERE p1.id = $id "
                "RETURN DISTINCT p2.id AS id"
            )
        else:
            query = (
                "MATCH (p1:Person)-[r]-(p2:Person) "
                "WHERE p1.id = $id "
                "RETURN DISTINCT p2.id AS id"
            )
        result = tx.run(query, id=id, direction=direction)
        return [row['id'] for row in result]
    
    with self.driver.session(database="neo4j") as session:
        result = session.execute_read(delete_relationship_tx, id, direction)
        print(result)