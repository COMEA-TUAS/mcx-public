#!/usr/bin/env python3
"""Script for simulating IOT measurement stream to ModelConductor experiment."""

import pandas as pd
import numpy as np
import sqlalchemy as sqla
from datetime import datetime as dt
from time import sleep, time
import logging
import sys, os, asyncio

from hbmqtt.client import MQTTClient, ConnectException
from hbmqtt.version import get_version
from docopt import docopt
from hbmqtt.utils import read_yaml_config
from hbmqtt.mqtt.constants import QOS_0, QOS_1, QOS_2

logger = logging.getLogger(__name__)
formatter = "[%(asctime)s] :: %(levelname)s - %(message)s"
logging.basicConfig(level=logging.DEBUG, format=formatter)

time_values = np.arange(0, 200, 0.02)
print(time_values.shape)
print(len(time_values))
n = len(time_values)
phi_cmd = np.zeros((n))
phi_cmd[int(n/4):int(n/2)] = -0.174532925199433
phi_cmd[int(n/2):] = +0.174532925199433

the_cmd = np.zeros((n))
psi_cmd = np.zeros((n))
alt_cmd = np.zeros((n)) + 3.048
# alt_cmd[int(n/4):int(3*n/4)] = 4.048

data = np.stack([time_values, phi_cmd, the_cmd, psi_cmd, alt_cmd], axis=-1)
print(data.shape)
data = pd.DataFrame(data, columns =["time","Phi_cmd","The_cmd","Psi_cmd","Alt_cmd"]) 

BROKER_URL = "mqtt://localhost:1883"

def main():
    if sys.version_info[:2] < (3, 4):
        logger.fatal("Error: Python 3.4+ is required")
        sys.exit(-1)

    config = None
    config = read_yaml_config(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'default_client.yaml'))
    logger.debug("Using default configuration")
    
    loop = asyncio.get_event_loop()

    client_id = "mqtt_publisher_exp"
    
    client = MQTTClient(client_id=client_id, config=config, loop=loop)
    try:
        logger.info("%s Connecting to broker" % client.client_id)

        loop.run_until_complete(client.connect(uri=BROKER_URL))
        qos = QOS_1
        topic = "topic_1"

        for _, row in data.iterrows():
            row['TIMING_client_request_timestamp'] = time()
            message = row.to_json().encode(encoding='utf-8')
            logger.info("%s Publishing to '%s'" % (client.client_id, topic))
            loop.run_until_complete(client.publish(topic, message, qos))
            sleep(0.01)
    except KeyboardInterrupt:
        loop.run_until_complete(client.disconnect())
        logger.info("%s Disconnected from broker" % client.client_id)
    except ConnectException as ce:
        logger.fatal("connection to '%s' failed: %r" % (BROKER_URL, ce))
    except asyncio.CancelledError as cae:
        logger.fatal("Publish canceled due to prvious error")



if __name__ == "__main__":
    main()