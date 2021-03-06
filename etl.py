import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import argparse

def load_staging_tables(cur, conn):
    """
    Copy song data and log data into staging tables.
    """
    for query in copy_table_queries.values():
        execute_query(cur, conn, query)


def insert_tables(cur, conn):
    """
    Populate sparkify tables from staging tables.
    """
    for query in insert_table_queries.values():
        execute_query(cur, conn, query)

def execute_query(cur, conn, query):
    """
    Execute specified SQL query.
    Used 
    """
    if __debug__:
        print("Executing ================================ Query")
        print(query)
    cur.execute(query)
    conn.commit()

def main():
    """
    Entry point of ETL script.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--copy_table',
                        help='Execute specified copy table statements.',
                        type=str,
                        choices=copy_table_queries.keys())
    parser.add_argument('-i','--insert_table',
                        help='Execute specified insert statements.',
                        type=str,
                        choices=insert_table_queries.keys())
    args = parser.parse_args()
    

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Copy specified tables
    if args.copy_table:
        for query in [q.strip() for q in args.copy_table.split(',')]:
                if query in copy_table_queries:
                    execute_query(cur, conn, copy_table_queries[query])

    # insert specified tables
    if args.insert_table:
        for query in [q.strip() for q in args.insert_table.split(',')]:
                if query in insert_table_queries:
                    execute_query(cur, conn, insert_table_queries[query])

    # Without command line arguments load and insert all tables
    if args.copy_table is None and args.insert_table is None:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()