import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in redshift

        Parameters:
                cur (obj): Create SQL execute
                conn (obj): Create DB connection

        Returns: N/A
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create new tables in redshift

        Parameters:
                cur (obj): Create SQL execute
                conn (obj): Create DB connection

        Returns: N/A
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Run this function if run create_tables.py

        Parameters: None

        Returns: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    for value in config['CLUSTER'].values():
        print(value)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
