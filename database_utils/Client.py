# -*- coding:utf-8 -*-
"""
Client class
"""
import psycopg2
from database_utils import helper, postgres_helper
import time


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
        t1 = time.time()
        ret = postgres_helper.fetchall(config=self.conf, sql=query)
        t2 = time.time()
        t = t2 - t1
        print('Finished in {:.2f} seconds.'.format(t))
        return ret

    def write(self, query):
        """
        Perform write actions.
        """
        t1 = time.time()
        postgres_helper.execute(conf=self.conf, query=query)
        t2 = time.time()
        t = t2 - t1
        print('Finished in {:.2f} seconds.'.format(t))
        return
