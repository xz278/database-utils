# -*- coding:utf-8 -*-
"""
Client class
"""
import psycopg2
from database_utils import helper, postgres_helper


class Client:

    def __init__(self, name):
        """
        Parameters
        ----------
        name: str
            Name of connections.
        """
        self.name = name
        conf = helper.read_database_conf()
        if name not in conf:
            raise Exception("no database connection with name '{}'".format(name))
        else:
            self.conf = conf[name]

    def read(self, query):
        """
        Performa query that read data ("select" statement)
        """
        return postgres_helper.fetchall(config=self.conf, sql=query)

    def write(self, query):
        """
        Perform write actions.
        """
        postgres_helper.execute(conf=self.conf, query=query)
        return
