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
        'host': '',
        'port': 5439,
        'dbname': 'test',
        'user': 'admin',
        'password': '123'
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


def generate_create_query(table_info, table_name):
    data_type = {
        'Date': 'DATE',
        'Int': 'INT8',
        'Double': 'FLOAT8',
        'String': 'VARCHAR(65535)'
    }

    create_template = """\
CREATE TABLE {} (
{}
)"""
    # comment
    comment_string = "/*\nQuery created on: {}\n*/\n".format(
        datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    )

    # query
    fields = ',\n'.join(
        ['    {} {}'.format(a, data_type[b].replace('65535', c)) if b == 'String' and c.isdigit()
         else '    {} {}'.format(a, data_type[b])
         for a, b, c in table_info[['field', 'type', 'type_param']].values]
    )
    return comment_string + create_template.format(table_name, fields)



def create_table(table_info, table_name, conf):
    """
    >>> create_table(table_info=table_info,
                         table_name='test_table',
                         conf=redshift_config)
    Parameters:
    -----------
    table_info: pandas.DataFrame
        Table schema, with columns ['field', 'type', 'type_param'].

    table_name: str
        Name of the table to be created.

    conf: dict
        Redshift connection configuration.
    """
    query = generate_create_query(table_info=table_info, table_name=table_name)
    execute(conf=conf, query=query)
    return


def generate_insert_query(table_name, data, table_info):

    insert_template = """
INSERT INTO {} ({})
VALUES
{};
"""
    # comment
    comment_string = "/*\nQuery created on: {}\n*/\n".format(
        datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    )
    data = data.copy()
    for c in table_info.loc[table_info['type'] == 'String', 'field']:
        data[c] = data[c].apply(lambda x: x if pd.isnull(x) else "'{}'".format(x))

    for f, t in table_info.loc[table_info['type'] == 'Date', ['field', 'type_param']].values:
        data[f] = data[f].apply(lambda x: x if pd.isnull(x) else "to_date('{}', '{}')".format(x, t))

    for c in table_info.loc[table_info['type'].isin(['Double', 'Int']), 'field']:
        data[c] = data[c].apply(lambda x: x if pd.isnull(x) else x.replace(',', '') if isinstance(x, str) else x)

    data = data.fillna('NULL')

    columns = ', '.join(data.columns.values)

    values = ',\n'.join(['({})'.format(', '.join(x)) for x in data.values])

    return comment_string + insert_template.format(table_name, columns, values)



def insert_into_rdf(data, table_info, table_name, conf):
    """
    >>> insert_into_rdf(data=data,
                        table_info=table_info,
                        table_name='test_table',
                        conf=redshift_config)
    Parameters:
    -----------
    data: pandas.DataFrame
        Data to insert.

    table_info: pandas.DataFrame
        Table schema, with columns ['field', 'type', 'type_param'].

    table_name: str
        Name of the table to be inserted into.

    conf: dict
        Redshift connection configuration.
    """
    query = generate_insert_query(table_name=table_name,
                                  data=data,
                                  table_info=table_info)
    execute(conf=conf, query=query)
    return
