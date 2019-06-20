import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


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
    When a python program is executed, python interpreter starts executing code inside main function.
    three functions above would be executed and then the connection would be closed
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    
if __name__ == "__main__":
    """
    For python main function, we have to use if __name__ == '__main__' condition to execute.
    """
    main()