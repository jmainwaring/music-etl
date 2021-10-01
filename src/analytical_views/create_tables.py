import configparser
import psycopg2
from sql_queries import drop_table_queries, insert_records_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def insert_records(cur, conn):
    for query in insert_records_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('../redshift.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    insert_records(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()