def clear_database(self):
    query = "g.V().drop().iterate()"
    try:
        self.gremlin_conn.submit(query)
    except Exception as e:
        print(e)


def delete_paper(self, id):
    query = f"g.V().has('paper', 'id', '{id}').drop()"
    try:
        self.gremlin_conn.submit(query)
    except Exception as e:
        print(e)


def delete_author(self, id):
    query = f"g.V().has('author', 'id', '{id}').drop()"
    try:
        self.gremlin_conn.submit(query)
    except Exception as e:
        print(e)


def delete_reference(self, id1, id2):
    query = f"g.V().has('paper', 'id', '{id1}').outE('reference').where(otherV().has('paper', 'id', '{id2}')).drop()"
    try:
        self.gremlin_conn.submit(query)
    except Exception as e:
        print(e)


def delete_authorship(self, id1, id2):
    query = f"g.V().has('author', 'id', '{id1}').outE('authorship').where(otherV().has('paper', 'id', '{id2}')).drop()"
    try:
        self.gremlin_conn.submit(query)
    except Exception as e:
        print(e)
