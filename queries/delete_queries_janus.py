def clear_database(gremlin_conn):
    query = "g.V().drop().iterate()"
    try:
        gremlin_conn.submit(query)
    except :
        print("Something went wrong with deletion")
    else:
        print("GraphDB deleted")
    return

def delete_paper(gremlin_conn,id):
    query = f"g.V().has('paper', 'id', {id}).drop()"
    try:
        gremlin_conn.submit(query)
    except:
        print("Something went wrong with deletion")
    else:
        print("Paper deleted")
    return

def delete_author(gremlin_conn,id):
    query = f"g.V().has('author', 'id', {id}).drop()"
    try:
        gremlin_conn.submit(query)
    except:
         print("Something went wrong with deletion")
    else:
        print("Author deleted")
    return

def delete_reference(gremlin_conn,id1,id2):
    query = f"g.V().has('paper', 'id', {id1}).outE('reference').where(otherV().has('paper', 'id', {id2})).drop()"
    try:
        gremlin_conn.submit(query)
    except:
        print("Something went wrong with deletion")
    else:
        print("Reference deleted")
    return

def delete_authorship(gremlin_conn,id1,id2):
    query = f"g.V().has('author', 'id', {id1}).outE('authorship').where(otherV().has('paper', 'id', {id2})).drop()"
    try:
        gremlin_conn.submit(query)
    except:
        print("Something went wrong with deletion")
    else:
        print("Authorship deleted")
    return


