import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    # connect to default database
    conn = psycopg2.connect(database="postgres", user="bro", password="n9l1b8r1p", host="127.0.0.1", port="5432")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparfify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to the default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(database="sparkifydb", user="bro", password="n9l1b8r1p", host="127.0.0.1", port="5432")
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur,conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur,conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur, conn = create_database()

    # drop tables if they exist
    drop_tables(cur,conn)

    # create tables if they exist
    create_tables(cur,conn)

    # close the connection
    conn.close()


if __name__ == "__main__":
    main()