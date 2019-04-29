import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    This function creates the sparkifydb dabases and connect to the database 
     
    argument: none
      
    return:
    cur: the cursor object
    conn : the connection object
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    This function drop tables.
    
    argument:
    cur: the cursor object
    conn : the connection object
    
    return :
    none
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates tables 
     
    argument:
    cur: the cursor object
    conn : the connection object
    
    return :
    none
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    When a python program is executed, python interpreter starts executing code inside it.
    three functions above would be executed and then the connection would be closed
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    """
    For python main function, we have to use if __name__ == '__main__' condition to execute.
    """
    main()