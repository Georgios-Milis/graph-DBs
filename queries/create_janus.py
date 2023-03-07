def create_paper(self, id, Title, year, n_citation):
    query = f"g.addV('paper').property('id', '{id}').property('title', '{Title}').property('year', {year}).property('n_citation', {n_citation})"
    try:
        response = self.gremlin_conn.submit(query).next()
    except Exception as e:
        response = e
    return response


def create_author(self, id, name, org):
    query = f"g.addV('author').property('id', '{id}').property('name', '{name}').property('org', '{org}')"
    try:
        response = self.gremlin_conn.submit(query).next()
    except Exception as e:
        response = e
    return response


def create_reference(self, id1, id2):
    query = f"g.addE('reference').from(__.V().has('paper', 'id', '{id1}')).to(__.V().has('paper', 'id', '{id2}'))"
    try:
        response = self.gremlin_conn.submit(query).next()
    except Exception as e:
        response = e
    return response


def create_authorship(self, id1, id2):
    query = f"g.addE('authorship').from(__.V().has('author', 'id', '{id1}')).to(__.V().has('paper', 'id', '{id2}'))"
    try:
        response = self.gremlin_conn.submit(query).next()
    except Exception as e:
        response = e
    return response
