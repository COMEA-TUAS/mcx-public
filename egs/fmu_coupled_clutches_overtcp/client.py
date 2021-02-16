#!/usr/bin/env python3
"""Script for simulating IOT measurement stream to ModelConductor experiment."""

import pandas as pd
import sqlalchemy as sqla
from datetime import datetime as dt
from time import sleep
data = pd.read_csv('CoupledClutches_in_sh.csv', delimiter=',')
from socket import AF_INET, socket, SOCK_STREAM


def send(my_msg, event=None):  # event is passed by binders.
    """Handles sending of messages."""
    client_socket.send(bytes(my_msg, "utf8"))
    if my_msg == "{quit}":
        client_socket.close()


HOST = "127.0.0.1"
PORT = 33003
BUFSIZ = 1024
ADDR = (HOST, PORT)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect(ADDR)

#---Main loop
from random import randint
from time import sleep, time

for _, row in data.iterrows():
    row['TIMING_client_request_timestamp'] = time()
    print(_)
    print(row)
    my_msg = row.to_json()
    my_msg = "{:<10}".format(str(len(my_msg))) + my_msg
    print(my_msg[0:50], "...")
    send(my_msg)
    sleep(0.1)

send('{quit}')
