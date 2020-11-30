from cassandra.cluster import Cluster

cluster = Cluster(contact_points=['cassandra'], port=9042)
session = cluster.connect()
session.execute('DROP KEYSPACE IF EXISTS mykeyspace')
session.execute('CREATE KEYSPACE mykeyspace WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': 1}')
session.execute('USE mykeyspace')
session.execute('CREATE TABLE profiles(id int PRIMARY KEY, name text);')
session.execute(
    """
    INSERT INTO profiles (id, name)
    VALUES (%s, %s)
    """,
    (0, "Anna Conda")
)
session.execute(
    """
    INSERT INTO profiles (id, name)
    VALUES (%s, %s)
    """,
    (1, "Jack Pott")
)
session.execute(
    """
    INSERT INTO profiles (id, name)
    VALUES (%s, %s)
    """,
    (2, "Stella Kwayut")
)
session.execute(
    """
    INSERT INTO profiles (id, name)
    VALUES (%s, %s)
    """,
    (3, "Phil Down")
)
session.execute(
    """
    INSERT INTO profiles (id, name)
    VALUES (%s, %s)
    """,
    (4, "Hugh Mungus")
)
