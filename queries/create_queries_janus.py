def create_paper(gremlin_conn,id,Title,year,n_citation):
    query = f"g.addV('paper').property('id', {id}).property('title', {Title}).property('year', {year}).property('n_citation', {n_citation})"
    try:
        response = gremlin_conn.submit(query).next()
    except:
        response = "Something went wrong with vertice create" 
    return response


def create_author(gremlin_conn, id , name , org):
    query = f"g.addV('author').property('id', {id}).property('name', {name}).property('org', {org})"
    try:
        response = gremlin_conn.submit(query).next()
    except:
        response = "Something went wrong with vertice create" 
    return response


def create_reference(gremlin_conn,id1,id2):
    query = f"g.addE('reference').from(g.V().has('paper', 'id', {id1})).to(g.V().has('paper', 'id', {id2}))"
    try:
        response = gremlin_conn.submit(query).next()
    except:
        response = "Something went wrong with edge create" 
    return response


def create_authorship(gremlin_conn,id1,id2):
    query = f"g.addE('authorship').from(g.V().has('author', 'id', {id1})).to(g.V().has('paper', 'id', {id2}))"
    try:
        response = gremlin_conn.submit(query).next()
    except:
        response = "Something went wrong with edge create" 
    return response
