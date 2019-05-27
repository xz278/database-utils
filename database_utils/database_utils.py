# -*- coding:utf-8 -*-
"""
Utility functions for data base utils
"""
from database_utils import core, helper

def list_connection():
    """
    List connections in current configuration file
    """

    conf = helper.read_database_conf(helper.CONF_FILE_PATH)
    connection_names = list(conf.keys())

    return connection_names


def inspect_connection(name):
    """
    Return detail configuration for the connections.
    """
    conf = helper.read_database_conf(helper.CONF_FILE_PATH)
    if name not in conf:
        print("no database connection with name '{}'".format(name))
        return None
    else:
        return conf[name]
