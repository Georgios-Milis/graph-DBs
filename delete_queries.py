def delete_person(self, id):
    """
    Delete the node corresponding to the person with the 
    id passed as argument and all the incident edges.
    """
    def delete_person_tx(tx, id):
        query = (
            "MATCH (p:Person) "
            "WHERE p.id = $id "
            "DETACH DELETE p"
        )
        tx.run(query, id=id)
    
    with self.driver.session(database="neo4j") as session:
        session.execute_write(delete_person_tx, id)


def delete_relationship(self, relationship, directed=True):
    """
    Delete the edge corresponding to the relationship passed as argument.
    If directed is True, then the edge is deleted only if input direction 
    matches the existing one in the graph. Else, the edge is deleted
    irrespective of direction.
    """
    def delete_relationship_tx(tx, relationship, directed):
        query = (
            "MATCH (p1:Person)-[r]->(p2:Person) "
            "WHERE p1.id = $relationship[0] AND p2.id = $relationship[1] "
            "DELETE r"
        ) if directed else (
            "MATCH (p1:Person)-[r]-(p2:Person) "
            "WHERE p1.id = $relationship[0] AND p2.id = $relationship[1] "
            "DELETE r"
        )
        tx.run(query, relationship=relationship, directed=directed)
    
    with self.driver.session(database="neo4j") as session:
        session.execute_write(delete_relationship_tx, relationship, directed)