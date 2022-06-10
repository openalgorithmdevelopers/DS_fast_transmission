

import pandas as pd
import numpy as np
import random

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
outTopic = "/DS/flask"      # for sending data through flask
inTopic = "/DS/Arduino"     # for receiving message from Arduino

fileNumber = 0

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'emqx'
password = 'public'

flagI = 0
  
def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

        
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        message = msg.payload.decode()
        print(f"Received " + message + "` from `{msg.topic}` topic")

    client.subscribe(inTopic, 1)
    client.on_message = on_message

import time;
def publish(client):
    msg_count = 0
    while True:
        time.sleep(1)
        msg = f"messages: {msg_count}"
        result = client.publish(outTopic, msg)
        # result = client.publish("topic", msg)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{outTopic}`")
        else:
            print(f"Failed to send message to topic {outTopic}")
        msg_count += 1   


def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)   

if __name__ == '__main__':
    run()