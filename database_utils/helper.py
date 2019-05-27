# -*- coding:utf-8 -*-
"""
Helper utility functions
"""
# -*- coding: utf-8 -*-
"""
Helper functions.
"""
import configparser
import os


CONF_FILE_PATH = os.path.join(os.path.expanduser('~'), '.database_utils')
CONF_FILE = os.path.join(CONF_FILE_PATH, 'conf.ini')


def gen_conf(data, section, fp):
    """
    Write configurations in 'data' to file 'fp'.

    Parameters:
    -----------
    data: dict
        Configurations.
    section: string
        'section' in configparser
    fp: string
        Configuration file path.
    """
    cp = configparser.ConfigParser()
    cp.add_section(section)
    for k, v in data.items():
        cp.set(section, k, str(v))
    with open(fp, 'w') as f:
        cp.write(f)
    return


def load_conf(fp):
    """
    Load a configuration file.

    Parameters:
    -----------
    fp: string
        File path.

    Returns:
    --------
    dict
    """
    cf = configparser.ConfigParser()
    cf.read(fp)

    sections = cf.sections()
    conf = {}
    for s in sections:
        conf[s] = {o: cf.get(section=s, option=o) for o in cf.options(s)}

    return conf


def create_conf_file():
    os.makedirs(CONF_FILE_PATH, exist_ok=True)
    with open(CONF_FILE, 'w') as f:
        pass


def conf_exist():
    return os.path.exists(CONF_FILE)


def read_database_conf():
    if not conf_exist():
        create_conf_file()
    return load_conf(CONF_FILE)
