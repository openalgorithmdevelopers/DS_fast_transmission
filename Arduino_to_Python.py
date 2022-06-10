# -*- coding: utf-8 -*-
"""
Created on Sat May 28 11:06:40 2022

@author: bhupendra.singh
"""
import random
import pandas as pd

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
#topic = "python/mqtt"
topic = "outSignal"

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'emqx'
password = 'public'

def print_to_csv(data, fileNo):
    fileName = 'DS_folder/DS' + str(fileNo) + '.csv'
    df = pd.DataFrame(data, columns=["time", "signal"])
    print(df)
    df.to_csv(fileName, index = False)
    print("Created file as " + fileName)
#    createAudioFile()
  
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

dataList = list()
keySet = set()
fileNumber = 0
def getSignalFromMsg(message):
    global fileNumber 
    print(fileNumber)
    keyDataPair = message.split(',')
    for i in keyDataPair:
        tmp = list()
    #        print(i)
        k = i.split(":")
#        print(k)
        if( len(k) == 2):  
            if(k[0] ==''):
                continue
            if(k[1] ==''):
                continue

            key = int(k[0])
            if(key in keySet):
                continue
            keySet.add(key)
            print("size of keyset")
            print(len(keySet))
            value = int(k[1])

            tmp.append(key)
            tmp.append(value) 

            dataList.append(tmp)
            if(len(keySet) > 9800):
                continue            
            if(len(keySet)%10 == 0):
                print(fileNumber)
                print_to_csv(dataList, fileNumber)
                fileNumber += 1
        
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        message = msg.payload.decode()
        getSignalFromMsg(message)

    client.subscribe(topic, 1)
    client.on_message = on_message

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()
