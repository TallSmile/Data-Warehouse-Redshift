import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import argparse

def drop_tables(cur, conn):
    for query in drop_table_queries.values():
        execute_query(cur, conn, query)


def create_tables(cur, conn):
    for query in create_table_queries.values():
        execute_query(cur, conn, query)

def execute_query(cur, conn, query):
    if __debug__:
        print("Executing ================================ Query")
        print(query)
    cur.execute(query)
    conn.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c','--create_table',
                        help='Execute specified create table statements.',
                        type=str,
                        choices=create_table_queries.keys())
    parser.add_argument('-d','--drop_table',
                        help='Execute specified drop table statements.',
                        type=str,
                        choices=drop_table_queries.keys())

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop specified tables
    if args.drop_table:
        for query in [q.strip() for q in args.drop_table.split(',')]:
                if drop_table_queries.has_key(query):
                    execute_query(cur, conn, drop_table_queries[query])
    # Create specified tables
    if args.create_table:
        for query in [q.strip() for q in args.create_table.split(',')]:
                if create_table_queries.has_key(query):
                    execute_query(cur, conn, create_table_queries[query])
    
    # If arguments are not specified drop and create all tables
    if args.create_table is None and args.drop_table is None:
        drop_tables(cur, conn)
        create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()