import paho.mqtt.client as mqtt
import json, time
import redis
import datetime
import sys

r = redis.StrictRedis(host = '127.0.0.1', port = 6379, decode_responses = True)

##MQTT Connection

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to mqtt server"+str(rc))
        client.subscribe("test/key1")
    else:
        print("Failed to connected", rc)

#received message from server

def on_message(client, userdata, msg):
    # print(msg.topic + " " +msg.payload.decode('utf-8'))
    message = msg.payload.decode('utf-8')
    #message=0
    # Once msg come into mqtt client, we then store the msg into redis
    key = str(datetime.datetime.now())
    print('key : {%s}, value : {%s} will be store into redis index 0' %(key,message))
    r.set(key, message)
    print(r.get(key)) # here you should get message value which you receive in mqtt
    

def on_publish(client, userdata, mid):
    print("mid: "+str(mid))


#connection setting
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish

#set connection info
client.connect("120.126.16.88", 1883)
client.loop_forever()

