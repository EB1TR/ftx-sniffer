# -*- coding: utf-8 -*-
# pylint: disable=locally-disabled, multiple-statements
# pylint: disable=fixme, line-too-long, invalid-name
# pylint: disable=W0703
# pylint: disable=W0605

__author__ = 'EB1TR'

# Libreria estándar ----------------------------------------------------------------------------------------------------
#
import sys
import json
import time
# ----------------------------------------------------------------------------------------------------------------------

# Paquetes instalados --------------------------------------------------------------------------------------------------
#
import mysql.connector
import paho.mqtt.client as mqtt
import maidenhead as mh
import geopy.distance
# ----------------------------------------------------------------------------------------------------------------------

# Importaciones locales ------------------------------------------------------------------------------------------------
#
import settings
# ----------------------------------------------------------------------------------------------------------------------

# Entorno --------------------------------------------------------------------------------------------------------------
#
try:
    VERSION = "20250304T0900Z"
    SQL = bool(settings.Config.SQL)
    DB_PASS = settings.Config.DB_PASS
    DB_HOST = settings.Config.DB_HOST
    DB_NAME = settings.Config.DB_NAME
    DB_USER = settings.Config.DB_USER
    MQTT_HOST = settings.Config.MQTT_HOST
    MQTT_PORT = settings.Config.MQTT_PORT
    MQTT_KEEP = settings.Config.MQTT_KEEP
    MQTT_RBN = settings.Config.MQTT_RBN
    TRACKING = settings.Config.TRACKING.split(',')
    SONDA = settings.Config.SONDA
    RX = bool(settings.Config.RX)
    TX = bool(settings.Config.TX)
    RBN = bool(settings.Config.RBN)
    PSK = bool(settings.Config.PSK)
    COMPLETE = {}
    # Carga de DXCC para continentes -----------------------------------------------------------------------------------
    #
    dxcc_dict = {}
    with open('dxcc.json', encoding="utf8") as f:
        dxcc = json.load(f)
        for el in dxcc['dxccure']:
            dxcc_dict[el['adif']] = el['cont']
    # ------------------------------------------------------------------------------------------------------------------
except Exception as e:
    print('Excepción: %s' % e)
    exit(1)
# ----------------------------------------------------------------------------------------------------------------------


# Locator, ADIF y continente de BDD --------------------------------------------------------------------------------------------------------------
#
def loc_hist():
    try:
        print(f'Consultando histórico de DXCC, Locator y Continentes conocidos')
        my_db = mysql.connector.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, database=DB_NAME)
        my_cursor = my_db.cursor()
        sql = ('SELECT `call`, `loc`, `adi`, `cont` from (SELECT `sc` as "call", `sl` as "loc", `sa` as "adi", `sco` '
               'as "cont" from spots group by `sc` UNION ALL SELECT `rc`, `rl`, `ra`, `rco` from spots group by `rc`) '
               'as A where `loc` != "" and `adi` != "NULL" and `cont` != "" order by `call` asc')
        my_cursor.execute(sql)
        result = my_cursor.fetchall()
        for e in result:
            COMPLETE[e[0]] = {'loc': e[1], 'cont': e[3], 'adif': e[2]}
        my_cursor.close()
        my_db.close()
        print(f'Se han obtenido datos para {len(result)} indicativos')
    except Exception as e:
        print(e)
# ----------------------------------------------------------------------------------------------------------------------


# Persistencia a BDD GENERAL---------------------------------------------------------------------------------------------------
#
def to_db_general(dato):
    try:
        my_db = mysql.connector.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, database=DB_NAME)
        my_cursor = my_db.cursor()
        sql_cam = f'`md`, `rp`, `t`, `sc`, `sl`, `rc`, `rl`, `sa`, `ra`, `b`, `d`, `sco`, `rco`, `or`, `target`'
        sql_val = (
            f'"{dato["md"]}", {dato["rp"]}, {dato["t"]}, "{dato["sc"]}", "{dato["sl"]}", "{dato["rc"]}", '
            f'"{dato["rl"]}", "{dato["sa"]}", "{dato["ra"]}", "{dato["b"]}", "{dato["d"]}", '
            f'"{dato['sco']}", "{dato['rco']}", "{dato['or']}", "{dato['target']}"')
        sql = f'INSERT INTO spots ({sql_cam}) VALUES ({sql_val})'
        my_cursor.execute(sql)
        my_db.commit()
        my_cursor.close()
        my_db.close()
    except Exception as e:
        print('Excepción en persistencia a BDD GENERAL: %s' % e)
        print(dato)
# ----------------------------------------------------------------------------------------------------------------------


# Persistencia a BDD SONDA RX---------------------------------------------------------------------------------------------------
#
def to_db_sonda(dato):
    try:
        my_db = mysql.connector.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, database=DB_NAME)
        my_cursor = my_db.cursor()
        sql_cam = f'`md`, `rp`, `t`, `sc`, `sl`, `rc`, `rl`, `sa`, `ra`, `b`, `d`, `sco`, `rco`, `or`, `target`'
        sql_val = (
            f'"{dato["md"]}", {dato["rp"]}, {dato["t"]}, "{dato["sc"]}", "{dato["sl"]}", "{dato["rc"]}", '
            f'"{dato["rl"]}", "{dato["sa"]}", "{dato["ra"]}", "{dato["b"]}", "{dato["d"]}", '
            f'"{dato['sco']}", "{dato['rco']}", "{dato['or']}", "{dato['target']}"')
        sql = f'INSERT INTO sonda_rx ({sql_cam}) VALUES ({sql_val})'
        my_cursor.execute(sql)
        my_db.commit()
        my_cursor.close()
        my_db.close()
    except Exception as e:
        print('Excepción en persistencia a BDD SONDA RX: %s' % e)
        print(dato)
# ----------------------------------------------------------------------------------------------------------------------


# Acciones al conectar a MQTT ------------------------------------------------------------------------------------------
#
def on_connect_a(client, userdata, flags, rc, fe):
    try:
        print("MQTT A: Conectado")
        if len(TRACKING) > 0:
            for e in TRACKING:
                if TX and RX:
                    client.subscribe(f'pskr/filter/v2/+/+/{e}/#')
                    client.subscribe(f'pskr/filter/v2/+/+/{e}.P/#')
                    client.subscribe(f'pskr/filter/v2/+/+/{e}.R/#')
                    client.subscribe(f'pskr/filter/v2/+/+/+/{e}/#')
                    client.subscribe(f'pskr/filter/v2/+/+/+/{e}.P/#')
                    client.subscribe(f'pskr/filter/v2/+/+/+/{e}.R/#')
                elif RX and not TX:
                    client.subscribe(f'pskr/filter/v2/+/+/+/{e}/#')
                    client.subscribe(f'pskr/filter/v2/+/+/+/{e}.P/#')
                    client.subscribe(f'pskr/filter/v2/+/+/+/{e}.R/#')
                elif TX and not RX:
                    client.subscribe(f'pskr/filter/v2/+/+/{e}/#')
                    client.subscribe(f'pskr/filter/v2/+/+/{e}.P/#')
                    client.subscribe(f'pskr/filter/v2/+/+/{e}.R/#')
                else:
                    print('Parámetro incorrecto de PATH')
                    exit(1)
            print("MQTT A: Topics suscritos")
        else:
            print('No hay indicativos a trackear.')
            pass

    except Exception as e:
        print('Excepción en conexión A: %s' % e)
        exit(1)
# ----------------------------------------------------------------------------------------------------------------------


# Acciones al conectar a MQTT ------------------------------------------------------------------------------------------
#
def on_connect_b(client, userdata, flags, rc, fe):
    try:
        print("MQTT B: Conectado")
        if len(TRACKING) > 0:
            for e in TRACKING:
                if TX and RX:
                    client.subscribe(f'rbn/+/+/{e}/#')
                    client.subscribe(f'rbn/+/+/{e}.P/#')
                    client.subscribe(f'rbn/+/+/{e}.R/#')
                    client.subscribe(f'rbn/+/+/+/{e}/#')
                    client.subscribe(f'rbn/+/+/+/{e}.P/#')
                    client.subscribe(f'rbn/+/+/+/{e}.R/#')
                elif RX and not TX:
                    client.subscribe(f'rbn/+/+/+/{e}/#')
                    client.subscribe(f'rbn/+/+/+/{e}.P/#')
                    client.subscribe(f'rbn/+/+/+/{e}.R/#')
                elif TX and not RX:
                    client.subscribe(f'rbn/+/+/{e}/#')
                    client.subscribe(f'rbn/+/+/{e}.P/#')
                    client.subscribe(f'rbn/+/+/{e}.R/#')
                else:
                    print('Parámetro incorrecto de PATH')
                    exit(1)
            print("MQTT B: Topics suscritos")
        else:
            print('No hay indicativos a trackear.')
            pass

    except Exception as e:
        print('Excepción: %s' % e)
        exit(1)
# ----------------------------------------------------------------------------------------------------------------------


# Acciones al recibir mensaje MQTT PSKR --------------------------------------------------------------------------------
#
def on_message_a(client, userdata, msg):
    try:
        global COMPLETE
        data = msg.payload
        dato = json.loads(data, strict=False)
        dato['d'] = int(geopy.distance.geodesic(mh.to_location(dato['sl'][:8]), mh.to_location(dato['rl'][:8])).km)
        dato['or'] = "PSK"
        dato['sco'] = dxcc_dict[dato['sa']]
        dato['rco'] = dxcc_dict[dato['ra']]
        dato['sl'] = dato['sl'][:4]
        dato['rl'] = dato['rl'][:4]
        dato['b'] = int(dato['b'].replace('cm','').replace('mm','').replace('m',''))
        dato.pop('sq')
        dato.pop('f')

        if msg.topic.split('/')[5].split('.')[0] in TRACKING and msg.topic.split('/')[6].split('.')[0] in TRACKING:
            dato['target'] = ''
        elif msg.topic.split('/')[5].split('.')[0] in TRACKING and not msg.topic.split('/')[6].split('.')[0] in TRACKING:
            dato['target'] = msg.topic.split('/')[5]
        elif not msg.topic.split('/')[5].split('.')[0] in TRACKING and msg.topic.split('/')[6].split('.')[0] in TRACKING:
            dato['target'] = msg.topic.split('/')[6]
        else:
            dato['target'] = ''

        dato = dict(sorted(dato.items()))

        if SQL and dato['b'] in [160, 80, 60, 40, 30, 20, 17, 15, 12, 10]:
            to_db_general(dato)
            if dato['rc'] == SONDA:
                to_db_sonda(dato)
            if RBN:
                COMPLETE[dato['sc']] = {'loc': dato['sl'], 'cont': dato['sco'], 'adif': dato['sa']}
                COMPLETE[dato['rc']] = {'loc': dato['rl'], 'cont': dato['rco'], 'adif': dato['ra']}
            print(f'PSK: {dato}')
    except Exception as e:
        print("PSK: " + str(e))
        print(msg.payload)
# ----------------------------------------------------------------------------------------------------------------------


# Acciones al recibir mensaje MQTT RBN ---------------------------------------------------------------------------------
#
def on_message_b(client, userdata, msg):
    global COMPLETE
    try:
        data = msg.payload
        data = json.loads(data, strict=False)

        if (data['dx'] in TRACKING and TX) or (data['src'] in TRACKING and RX):
            try:
                data['sa'] = COMPLETE[data['dx']]['adif']
                data['sco'] = COMPLETE[data['dx']]['cont']
                data['sl'] = COMPLETE[data['dx']]['loc']
            except:
                data['sa'] = 999
                data['sco'] = ''
                data['sl'] = ''

            try:
                data['ra'] = COMPLETE[data['src']]['adif']
                data['rco'] = COMPLETE[data['src']]['cont']
                data['rl'] = COMPLETE[data['src']]['loc']
            except:
                data['ra'] = 999
                data['rco'] = ''
                data['rl'] = ''

            dato = {
                'target': '',
                'md': data['mode'],
                'rp': data['db'],
                't': data['tstamp'],
                'sc': data['dx'],
                'sl': data['sl'],
                'rc': data['src'],
                'rl': data['rl'],
                'sa': data['sa'],
                'ra': data['ra'],
                'b': data['band'],
                'd': 0,
                'or': 'RBN',
                'sco': data['sco'],
                'rco': data['rco']
            }

            if dato['rl'] != "" and dato['sl'] != "":
                dato['d'] = int(geopy.distance.geodesic(mh.to_location(dato['sl']), mh.to_location(dato['rl'])).km)

            if data['dx'].split('.')[0] in TRACKING and data['src'].split('.')[0] in TRACKING:
                dato['target'] = ''
            elif data['dx'].split('.')[0] in TRACKING and not data['src'].split('.')[0] in TRACKING:
                dato['target'] = data['dx']
            elif not data['dx'].split('.')[0] in TRACKING and data['src'].split('.')[0] in TRACKING:
                dato['target'] = data['src']
            else:
                dato['target'] = ''

            dato = dict(sorted(dato.items()))

            if SQL and dato['b'] in [160, 80, 60, 40, 30, 20, 17, 15, 12, 10]:
                to_db_general(dato)
                if dato['rc'] == "EA1HFI.P":
                    to_db_sonda(dato)
                print(f'RBN: {dato}')
    except Exception as e:
        print("RBN: " + str(e))
        print(msg.payload)
# ----------------------------------------------------------------------------------------------------------------------


if __name__ == '__main__':
    try:
        # Mensaje de inicio a "status.log" -----------------------------------------------------------------------------
        #
        print(f'Arrancando PSKREPORTER SNIFFER V{VERSION}')
        # --------------------------------------------------------------------------------------------------------------

        # Orígenes a analizar ------------------------------------------------------------------------------------------
        #
        print(f'Configurando orígenes:')
        if PSK and RBN:
            print(f'PSKReporter:          ✅')
            print(f'ReverseBeaconNetwork: ✅')
        elif PSK and not RBN:
            print(f'PSKReporter:          ✅')
            print(f'ReverseBeaconNetwork: ❌')
        elif not PSK and RBN:
            print(f'PSKReporter:          ❌')
            print(f'ReverseBeaconNetwork: ✅')
        else:
            print('Parámetro incorrecto sin orígenes para analizar.')
            exit(1)
        # --------------------------------------------------------------------------------------------------------------

        # Objetivos a trackear -----------------------------------------------------------------------------------------
        #
        print(f'\nConfiguranto tipos de tramas:')
        if TX or RX:
            if RX:
                print(f'RX de los trackeados: ✅')
            else:
                print(f'RX de los trackeados: ❌')
            if TX:
                print(f'TX de los trackeados: ✅')
            else:
                print(f'TX de los trackeados: ❌')
        else:
            print('Parámetro incorrecto de PATH')
            exit(1)
        # --------------------------------------------------------------------------------------------------------------

        # Objetivos a trackear -----------------------------------------------------------------------------------------
        #
        print(f'\nConfiguranto objetivos:')
        calls = ""
        for e in TRACKING:
            calls += f'{e}, '
        calls = calls[:-2]
        print(f'Indicativos: {calls}')
        # --------------------------------------------------------------------------------------------------------------

        # Mensaje de inicio a "status.log" -----------------------------------------------------------------------------
        #
        print(f'\nLanzando conexiones:')
        # --------------------------------------------------------------------------------------------------------------

        # Instancia MQTT -----------------------------------------------------------------------------------------------
        #
        if PSK:
            client1 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            client1.on_connect = on_connect_a
            client1.on_message = on_message_a
            try:
                client1.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            except Exception as e:
                print(f"Error al conectar con el primer broker: {e}")
            client1.loop_start()

        if RBN:
            # Historíco de indicativos ---------------------------------------------------------------------------------
            #
            loc_hist()
            # ----------------------------------------------------------------------------------------------------------

            client2 = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            client2.on_connect = on_connect_b
            client2.on_message = on_message_b
            try:
                client2.connect(MQTT_RBN, 1883, keepalive=60)
            except Exception as e:
                print(f"Error al conectar con el segundo broker: {e}")
            client2.loop_start()

        try:
            while True:
                time.sleep(0.05)
        except KeyboardInterrupt:
            print("Cerrando conexiones...")
            if PSK:
                client1.loop_stop()
                client1.disconnect()
            if RBN:
                client2.loop_stop()
                client2.disconnect()
        # --------------------------------------------------------------------------------------------------------------

    # Condiciones de salida --------------------------------------------------------------------------------------------
    #
    except KeyboardInterrupt:
        print("Parando: Usuario")
        sys.exit(0)
    except EOFError:
        text = 'EOFError: %s' % EOFError
        print(text)
        print("Parando: EOFError")
        sys.exit(0)
    except OSError:
        text = 'OSError: %s' % OSError
        print("Parando: OSError")
        print(text)
        sys.exit(0)
    except Exception as e:
        text = 'Excepción general: %s' % e
        print("Parando: General")
        print(text)
        sys.exit(0)
    # ------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
