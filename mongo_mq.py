import paho.mqtt.client as mqtt
import json, time
import datetime
import sys
import pymongo



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
    receiveTime = str(datetime.datetime.now())
    mydata = {'receiveTime' : receiveTime, 'value' : message}
    print(mydata)
    
    mongo_client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    mydb = mongo_client["testMongoDB"]
    db1st = mongo_client.list_database_names()
    
    mycol = mydb["testMongoCol"]
    collst = mydb.list_collection_names()
    
    #mydata = {'receiveTime' : receiveTime, 'value' : message}
    testData = mycol.insert_one(mydata)
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

