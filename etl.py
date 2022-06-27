from cgitb import reset
import configparser
import psycopg2
import sql_queries as sql

def load_staging_tables(cur, conn):
    for query in sql.copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in sql.insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # print(cur)

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()