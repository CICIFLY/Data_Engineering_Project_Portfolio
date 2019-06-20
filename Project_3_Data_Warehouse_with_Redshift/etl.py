import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function load data from S3 to staging tables on redshift
     
    argument:
    cur: the cursor object
    conn : the connection object
    
    return :
    none
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function load data from staging tables to analytics tables on redshift
     
    argument:
    cur: the cursor object
    conn : the connection object
    
    return :
    none
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This main function will extract all the required information to connect to the database and then 
    call load_staging_tables function to load data to staging tables and then execute insert_tables function to load data to
    analytics tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
    