def find_paper(gremlin_conn,id):
    query = f"g.V().has('paper', 'id', {id}).values('id')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No paper with id: {id}"
    return response

def title_of_paper(gremlin_conn,id):
    query = f"g.V().has('paper', 'id', {id}).values('title')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No paper with id: {id}"
    return response

def org_of_author(gremlin_conn,id):
    query = f"g.V().has('author', 'id', {id}).values('org')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No author with id: {id}"
    return response

def find_author(gremlin_conn,id):
    query = f"g.V().has('author', 'id', {id}).values('name')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No author with id: {id}"
    return response

def references_of(gremlin_conn,id):
    query = f"g.V().has('paper', 'id', {id}).out('reference').values('id')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No references from paper with id:{id}"
    return response

def references_to(gremlin_conn,id):
    query = f"g.V().has('paper', 'id', {id}).in('reference').values('id')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No references to paper with id:{id}"
    return response

def papers_of(gremlin_conn,id):
    query = f"g.V().has('author', 'id', {id}).out('authorship').values('id')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No author with id: {id}"
    return response


def authors_of(gremlin_conn,id):
    query = f"g.V().has('paper', 'id', {id}).in('authorship').values('id')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = f"No paper with id: {id}"
    return response

def are_collaborators(gremlin_conn,id1,id2):
    query = f"g.V().and(inE('authorship').where(otherV().has('author', 'id', {id1})), inE('authorship').where(otherV().has('author', 'id', {id2}))).values('id')"
    try:
        response = gremlin_conn.submit(query).next()
    except StopIteration:
        response = "No collaborations between authors with id1,id2:{id1},{id2}"
    return response
