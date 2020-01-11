import psycopg2

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

def main():
    cur, conn = create_database()

if __name__ == "__main__":
    main()