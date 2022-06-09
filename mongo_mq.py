import paho.mqtt.client as mqtt
import json, time
import datetime
import sys
import pymongo
import pandas as pd
import numpy as np


##MQTT Connection

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to mqtt server"+str(rc))
        client.subscribe("test/sub_topic")
    else:
        print("Failed to connected", rc)

#received message from server

def on_message(client, userdata, msg):
    
    message = str(msg.payload.decode('utf-8'))
    dic=eval(message)
    
    receiveTime = str(datetime.datetime.now())
    
    df2=pd.DataFrame(["SN","Time","MAC","SSID","IP","RSSI","BatteryLevel","isCharging","Mem"])
    test_df=df2.append(dic,ignore_index=True)
    test_df.insert(0,column="ReceiveTime",value=receiveTime)
    #print(test_df.head())
    print(test_df)
    #mydata = {'receiveTime' : receiveTime, 'value' : dic}
    #print(mydata)
    
    
    mongo_client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = mongo_client["testMongoDB"]
    #db1st = mongo_client.list_database_names()
    
    mycol = db["testMongoCol"]
    collist = db.list_collection_names()
    
    testData = mycol.insert_one(dic)
    print(testData)


def on_publish(client, userdata, mid):
    print("mid: "+str(mid))


#connection setting
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish

#set connection info
client.connect("120.126.18.132", 1883)
client.loop_forever()


