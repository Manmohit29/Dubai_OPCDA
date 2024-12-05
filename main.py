import json
import paho.mqtt.client as mqtt
import OpenOPC
import time
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
import os
import random
import sys
# # region Rotating Logs
import os
import logging
from logging.handlers import TimedRotatingFileHandler

# Get the directory of the executable file
if getattr(sys, 'frozen', False):
    dirname = os.path.dirname(sys.executable)
else:
    dirname = os.path.dirname(os.path.abspath(__file__))

log_level = logging.INFO

FORMAT = '%(asctime)-15s %(levelname)-8s %(module)-15s:%(lineno)-8s %(message)s'

logFormatter = logging.Formatter(FORMAT)
log = logging.getLogger("LOGS")

# Checking and creating logs directory here
log_dir = os.path.join(dirname, 'logs')
if not os.path.isdir(log_dir):
    log.info("[-] logs directory doesn't exist")
    try:
        os.mkdir(log_dir)
        log.info("[+] Created logs dir successfully")
    except Exception as e:
        log.error(f"[-] Can't create dir logs Error: {e}")

fileHandler = TimedRotatingFileHandler(os.path.join(log_dir, 'app_log'),
                                       when='midnight', interval=1)

fileHandler.setFormatter(logFormatter)
fileHandler.suffix = "%Y-%m-%d.log"
log.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
log.addHandler(consoleHandler)
log.setLevel(log_level)
# endregion

OPC_SERVER = "ABB.AC800MC_OpcDaServer.3"
# MQTT Broker Info
MQTT_BROKER_ADDRESS = "mqtt.infinite-uptime.com"
MQTT_BROKER_PORT = 1883
MQTT_TOPIC = "iu_external_device_data_v1"
GL_SEND_DATA = True
GL_CLIENT_ID = f'HIS-MQTT-{random.randint(0, 1000)}'
# GL_MACHINE_INFO = {
#     'NOTE': {
#         'pub_topic': 'STG011',
#         'sub_topic': 'TRIGGER_STG011',
#         'ip': '192.168.0.1',
#         'machine_id': '01',
#         'line': 'A'
#     }}

data = {
    'TRF ACTUAL': 'Applications.AA111Pilot.PilotCom.RollGapToPilot.TrfAct',
    'MILL MASTER CURRENT': 'Applications.AA111Pilot.PilotCom.Mill.Drv1.FromDrv.MotCurrent',
    'MILL MASTER TORQUE': 'Applications.AA111Pilot.PilotCom.Mill.Drv1.FromDrv.MotTqFilt',
    'MILL FOLLOWER CURRENT': 'Applications.AA111Pilot.PilotCom.Mill.Drv2.FromDrv.MotCurrent',
    'MILL FOLLOWER TORQUE': 'Applications.AA111Pilot.PilotCom.Mill.Drv2.FromDrv.MotTqFilt',
    'FUME EXHAUST CURRENT': 'Applications.AA111Pilot.Drives.FumeEx.MotorCurAct',
    'FUME EXHAUST SPEED': 'Applications.AA111Pilot.Drives.FumeEx.MotorSpeedAct',
    'FUME EXHAUST TORQUW': 'Applications.AA111Pilot.Drives.FumeEx.MotorTqAct',
    'POR  CURRENT': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint21',
    'POR  SPEED': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint22',
    'POR TORQUE %': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint23',
    'POR TENSION ACT': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint24',
    'POR DIA': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint25',
    'ETR CURRENT': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint28',
    'ETR SPEED': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint29',
    'ETR TORQUE %': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint30',
    'ETR TENSION ACT': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint31',
    'ETR DIA': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint32',
    'DTR CURRENT': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint35',
    'DTR SPEED': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint36',
    'DTR TORQUE %': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint37',
    'DTR TENSION ACT': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint38',
    'DTR DIAMETER': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint39',
    'MILL SPEED': 'Applications.AA111Pilot.Drives.EPPP.Out.Dint42'
}


# mqttc = None


# The callback for when the client receives a CONNACK response from the server.


def on_connect(client, userdata, flags, reason_code, properties):
    log.info(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(MQTT_TOPIC)


# The callback for when a PUBLISHING message is received from the server.


def on_message(client, userdata, msg):
    # log.info(msg.topic + " " + str(msg.payload))
    pass


# def try_connect_mqtt():
#     mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
#     mqttc.on_connect = on_connect
#     mqttc.on_message = on_message
#     for i in range(5):
#         try:
#             mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT)
#             if mqttc.is_connected():
#                 break
#         except Exception as e:
#             log.error(f"[-] Unable to connect to mqtt broker {e}")
#     try:
#         mqttc.loop_start()
#     except Exception as e:
#         log.error(f"[-] Error while starting loop {e}")
#     return mqttc

def try_connect_mqtt():
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    for i in range(5):
        try:
            mqttc.connect(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT)
            mqttc.loop_start()  # Start the loop after connecting
            time.sleep(1)  # Give some time to establish connection
            if mqttc.is_connected():
                break
        except Exception as e:
            log.error(f"[-] Unable to connect to MQTT broker: {e}")
            time.sleep(2)  # Wait before retrying

    return mqttc


def opcda_connect():
    try:
        opc = OpenOPC.client()
        opc.servers()
        opc.connect(OPC_SERVER)
        log.info("OPCDA Server connected")
        return opc
    except Exception as e:
        log.error(f"Error while connecting with opcda server: {e}")
        return None


def data_from_opcda(opc):
    global data

    opc_data = {
        "001_A": "0",
        "001_B": "0",
        "001_C": "0",
        "001_D": "0",
        "001_E": "0",
        "001_F": "0",
        "001_G": "0",
        "001_H": "0",
        "001_I": "0",
        "001_J": "0",
        "001_K": "0",
        "001_L": "0",
        "001_M": "0",
        "001_N": "0",
        "001_O": "0",
        "001_P": "0",
        "001_Q": "0",
        "001_R": "0",
        "001_S": "0",
        "001_T": "0",
        "001_U": "0",
        "001_V": "0",
        "001_W": "0",
        "001_X": "0",
    }
    for (k1, v1), (k2, v2) in zip(data.items(), opc_data.items()):
        # opc = opcda_connect()
        # if opc:
        for i in range(6):
            try:
                tag_data = opc[v1]
                log.info(f"{v1} : {tag_data}")
                # log.info("Communicating with the gateway (Success)")
                opc_data[k2] = f"{tag_data}"
                break
            except Exception as e:
                log.error(f"error while reading data : {e}")
                time.sleep(1)
        # opc.close()
    # else:
    #     log.info(f"OPC server is not connected")
    return opc_data


#
# def main():
#     try:
#         while True:
#             mqttc = try_connect_mqtt()
#             opc_da_data = data_from_opcda()
#             payload = {
#                 "DEVICEID": "D6:2B:74:C4:CF:BE",
#                 "TIMESTAMP": f"{time.time()}",
#                 "PROCESS_PARAMETER": opc_da_data
#             }
#             log.info(payload)
#             # Publish payload
#             status = mqttc.publish(MQTT_TOPIC, json.dumps(payload))
#             log.info(f"Status {status}")
#             time.sleep(5)  # Adjust interval as needed
#     except KeyboardInterrupt:
#         log.error("Program interrupted by user")
#     except Exception as e:
#         log.error(f"Error : {e}")
#     finally:
#         mqttc.loop_stop()
#         mqttc.disconnect()

def main():
    try:
        while True:
            mqttc = try_connect_mqtt()
            if mqttc.is_connected():
                opc_connect = opcda_connect()
                # while mqttc.is_connected():
                if opc_connect:
                    opc_da_data = data_from_opcda(opc_connect)
                    payload = {
                        "DEVICEID": "D6:2B:74:C4:CF:BE",
                        "TIMESTAMP": f"{time.time()}",
                        "PROCESS_PARAMETER": opc_da_data
                    }
                    log.info(payload)
                    # Publish payload
                    result = mqttc.publish(MQTT_TOPIC, json.dumps(payload))
                    status = result.rc
                    log.info(f"Publish status: {status}")
                    opc_connect.close()
                else:
                    log.info(f"OPC server is not connected")
                    # if mqttc.is_connected():
                mqttc.disconnect()
                mqttc.loop_stop()
            else:
                log.info("Mqtt is not connected")
            time.sleep(5)  # Adjust interval as needed
    except KeyboardInterrupt:
        log.error("Program interrupted by user")
    except Exception as e:
        log.error(f"Error: {e}")


# Start the main function
if __name__ == "__main__":
    main()
