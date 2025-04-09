# -*- coding: utf-8 -*-
# pylint: disable=locally-disabled, multiple-statements
# pylint: disable=fixme, line-too-long, invalid-name
# pylint: disable=W0703

""" Spots to MQTT for container format. Settings file. """

__author__ = 'EB1TR'

import sys
from os import environ, path
from environs import Env


ENV_FILE = path.join(path.abspath(path.dirname(__file__)), '.env')

try:
    ENVIR = Env()
    ENVIR.read_env()
except Exception as e:
    print('Error: .env file not found: %s' % e)
    sys.exit(1)


class Config:
    """
    This is the generic loader that sets common attributes

    :param: None
    :return: None
    """

    if environ.get('MQTT_HOST'):
        MQTT_HOST = ENVIR('MQTT_HOST')

    if environ.get('MQTT_RBN'):
        MQTT_RBN = ENVIR('MQTT_RBN')

    if environ.get('MQTT_PORT'):
        MQTT_PORT = int(ENVIR('MQTT_PORT'))

    if environ.get('MQTT_KEEP'):
        MQTT_KEEP = int(ENVIR('MQTT_KEEP'))

    if environ.get('SQL'):
        SQL = int(ENVIR('SQL'))

    if environ.get('DB_PASS'):
        DB_PASS = ENVIR('DB_PASS')

    if environ.get('DB_HOST'):
        DB_HOST = ENVIR('DB_HOST')

    if environ.get('DB_NAME'):
        DB_NAME = ENVIR('DB_NAME')

    if environ.get('DB_USER'):
        DB_USER = ENVIR('DB_USER')

    if environ.get('TRACKING'):
        TRACKING = ENVIR('TRACKING').upper()

    if environ.get('SONDA'):
        SONDA = ENVIR('SONDA')

    if environ.get('RX'):
        RX = int(ENVIR('RX'))

    if environ.get('TX'):
        TX = int(ENVIR('TX'))

    if environ.get('RBN'):
        RBN = int(ENVIR('RBN'))

    if environ.get('PSK'):
        PSK = int(ENVIR('PSK'))