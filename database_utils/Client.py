# -*- coding:utf-8 -*-
"""
Client class
"""
import psycopg2
from database_utils import helper, postgres_helper
import time


class Client:

    def __init__(self, name=None, conf=None):
        """
        Parameters
        ----------
        name: str
            Name of connections.
        """
        if name is None and conf is None:
            raise Exception("must specify one of the arguments: "
                            "'name' or 'conf'")
        if conf is not None:
            self.conf = conf
        else:
            self.name = name
            self.database = name.split('_')[0]
            conf = helper.read_database_conf()
            if name not in conf:
                raise Exception("no database connection with name '{}'".format(name))
            else:
                self.conf = conf[name]

    def read(self, query):
        """
        Performa query that read data ("select" statement)
        """
        t1 = time.time()
        if self.database in ['redshift', 'postgres']:
            ret = postgres_helper.fetchall(config=self.conf, sql=query)
        else:
            raise Exception("database not supported yet: '{}'"
                            .format(self.database))
        t2 = time.time()
        t = t2 - t1
        print('Finished in {:.2f} seconds.'.format(t))
        return ret

    def write(self, query):
        """
        Perform write actions.
        """
        t1 = time.time()
        if self.database in ['redshift', 'postgres']:
            postgres_helper.execute(conf=self.conf, query=query)
        else:
            raise Exception("database not supported yet: '{}'"
                            .format(self.database))
        t2 = time.time()
        t = t2 - t1
        print('Finished in {:.2f} seconds.'.format(t))
        return

    def create_table(self, table_info, table_name):
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
        t1 = time.time()
        if self.database in ['redshift', 'postgres']:
            postgres_helper.create_table(
                conf=self.conf,
                table_info=table_info,
                table_name=table_name
            )
        else:
            raise Exception("database not supported yet: '{}'"
                            .format(self.database))
        t2 = time.time()
        t = t2 - t1
        print('Finished in {:.2f} seconds.'.format(t))
        return

    def insert_dataframe(self, table_info, table_name, data):
        """
        Create table
        """
        t1 = time.time()
        if self.database in ['redshift', 'postgres']:
            postgres_helper.insert_into_rdf(
                conf=self.conf,
                table_info=table_info,
                table_name=table_name,
                data=data
            )
        else:
            raise Exception("database not supported yet: '{}'"
                            .format(self.database))
        t2 = time.time()
        t = t2 - t1
        print('Finished in {:.2f} seconds.'.format(t))
        return

    def list_tables(self, schema='public'):
        """
        List tables
        """
        if self.database in ['redshift', 'postgres']:
            tables = postgres_helper.list_tables(conf=self.conf, schema=schema)
        else:
            raise Exception("database not supported yet: '{}'"
                            .format(self.database))

        return tables

    def list_columns(self, table, schema=None):
        """
        List columns in a table
        """
        if self.database in ['redshift', 'postgres']:
            if schema is None:
                raise Exception("argument 'schema' is required when"
                                "database type is 'redshift' or 'postgres'")
            cols = postgres_helper.list_columns(
                conf=self.conf,
                schema=schema,
                table=table
            )
        else:
            raise Exception("database not supported yet: '{}'"
                            .format(self.database))

        return cols
