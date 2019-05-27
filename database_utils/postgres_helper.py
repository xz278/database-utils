# -*- coding:utf-8 -*-
"""
Helper utility functions
"""
# -*- coding: utf-8 -*-
"""
Helper functions for postgres connections.
"""
import psycopg2, traceback
import pandas as pd


def fetchall(config, sql):
    """
    Fetch data from PostgreSQL

    >>>redshift = {
        'host': 'dwh.cgqfpdw0lgfw.cn-northwest-1.redshift.amazonaws.com.cn',
        'port': 5439,
        'dbname': 'test',
        'user': 'mcc_dw_admin',
        'password': 'mj3HcEwC5rV37UmaKqhc'
    }
    >>>df = postgres_fetchall(redshift=config, sql='select * from loan;')

    Parameters:
    -----------
    config: dict
        Database credentials.

    sql: str
        Queries to execute.

    Returns:
    --------
    pandas.DataFrame
        Data retrieved.
    """
    cursor = None
    conn = None
    data = None
    try:
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        columns = [x[0] for x in cursor.description]
        data = pd.DataFrame(data, columns=columns)
    except:
        raise Exception(traceback.format_exc())
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()
    return data


def execute(conf, query):
    conn = None
    try:
        conn = psycopg2.connect(**conf)
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()