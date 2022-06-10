# -*- coding: utf-8 -*-
"""
Created on Sat May 28 11:06:40 2022

@author: bhupendra.singh
"""
import pandas as pd
import numpy as np
import random
from matplotlib.animation import FuncAnimation
import matplotlib.pyplot as plt
import pandas as pd

from paho.mqtt import client as mqtt_client


broker = 'broker.emqx.io'
port = 1883
#topic = "python/mqtt"
topic = "outSignal"

fileNumber = 0

# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'emqx'
password = 'public'

flagI = 0

def print_to_csv(data, fileNo):
    import pandas as pd
    fileName = 'DS' + str(fileNo) + '.csv'
    df = pd.DataFrame(data, columns=["time", "signal"])
    print(df)
    df.to_csv(fileName, index = False)
    print("Created file as " + fileName)
#    createAudioFile()
    
def createAudioFile():
    import numpy as np
    import pandas as pd
    from scipy.io.wavfile import write
    
    df = pd.read_csv('ds.csv')
    y = df['signal']
    data = np.array(y)
    
#    data = np.random.uniform(-1,1,44100) # 44100 random samples between -1 and 1
    scaled = np.int16(data/np.max(np.abs(data)) * 32767)
    write('test.wav', 1000, scaled) # sampling frequqency with which the audio is captured by mic
    
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

REC_SIGNAL = list()
dataList = list()
keySet = set()
fileNumber = 0
def getSignalFromMsg(message):    
    import matplotlib.pyplot as plt

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

            df = pd.read_csv("DS.csv")
            x = df['time']
            y = df['signal']

            k=2

            kern=np.ones(2*k+1)/(2*k+1)

            y=np.convolve(y,kern, mode='same')

            # print(len(x)) 
            plt.cla()       

            plt.plot(x, y)
            # plt.scatter(x,y)
            plt.draw()
            plt.pause(0.001)

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

        data = getSignalFromMsg(message)

        print(f"Received " + message + "` from `{msg.topic}` topic")

    client.subscribe(topic, 1)
    client.on_message = on_message

plt.ion()
plt.show()

def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()

if __name__ == '__main__':
    run()
