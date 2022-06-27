import paho.mqtt.client as mqtt
import json, time
import datetime
import sys
import pymongo
import pandas as pd
import numpy as np
import ast
from pymongo import MongoClient

import seaborn as sns
from sklearn.ensemble import IsolationForest


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
    dic=ast.literal_eval(message)
    
    receiveTime = str(datetime.datetime.now())
    
    df0=pd.DataFrame.from_dict(dic,orient='index')
    df_train=df0.append(dic,ignore_index=True)
    df_train.insert(0,column="ReceiveTime",value=receiveTime)
    print(df_train)
    
    #mydata = {'receiveTime' : receiveTime, 'value' : dic}
    #print(mydata)
   
    
    mongo_client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = mongo_client["testMongoDB"]
    db1st = mongo_client.list_database_names()
    
    mycol = db["testMongoCol"]
    collst = db.list_collection_names()
    
    #df_train = pd.DataFrame(list(mycol.find()))
    
#Read the message every 10 seconds
    
def time1():
    on_message()
    time.sleep(10)
    
    
    
##Anomaly Detection

#import numpy as np
#import pandas as pd
#import seaborn as sns
#from sklearn.ensemble import IsolationForest
#df_dict = json.loads(mycol)

#df_train = pd.DataFrame(list(mycol.find()))
#df_train = pd.DataFrame(list(df_dict), columns=['BatteryLevel','RSSI','Mem'])
#df_train = pd.DataFrame.from_dict(df_dict)
#df_train.info()
#df_train.head()
    
#boxplot
#sns.boxplot(df_train['BatteryLevel'])

#training model
    random_state = np.random.RandomState(42)
    model=IsolationForest(n_estimators=100,max_samples='auto',contamination=float(0.2),random_state=random_state)
    model.fit(df_train['BatteryLevel','RSSI','Mem'])
    #model.fit(df_train[['BatteryLevel','RSSI','Mem']])
    print(model.get_params())

#Train increase score + Anomaly data
#for i in range(0,49):
#    df_train.loc[i,'BatteryLevel']=200
    df_train['scores'] = model.decision_function(df_train['BatteryLevel','RSSI','Mem'])
    df_train['anomaly_score'] = model.predict(df_train['BatteryLevel','RSSI','Mem'])
    #df_train['scores'] = model.decision_function(df_train[['BatteryLevel','RSSI','Mem']])
    #df_train['anomaly_score'] = model.predict(df_train[['BatteryLevel','RSSI','Mem']])
#df_test[df_train['anomaly_score']==-1].head(50)

#TrainResult
    count = df_train.shape[0]
#    anomaly_count = 50
#    anomaly_count_correct = 0
#    anomaly_count_wrong = 0
    normal_count_correct = 0
    normal_count_wrong = 0
#for i in range(0,49):
#    if list(df_train['anomaly_score'])[i] == -1:
#        anomaly_count_correct += 1
#    else:
#        anomaly_count_wrong += 1
#accuracy_a = 100*(anomaly_count_correct/anomaly_count)
    for i in range(0,(df_train.shape[0]-1)):
        if list(df_train['anomaly_score'])[i] == 1:
            normal_count_correct += 1
        else:
            normal_count_wrong += 1
    accuracy_n = 100*(normal_count_correct/count)
#print("Accuracy of the model(only_anomaly): ", accuracy_a)
    print("Accuracy of the model(only_normal): ", accuracy_n)
#print("Anomaly Count_a: " + anomaly_count_wrong)
    print("Anomaly Count_n: " + normal_count_wrong)

##df_test
#df_dict2 = json.loads(dic)
#df_test = pd.DataFrame.from_dict(df_dict2)

##time
#import time

#def sleeptime(hour,min,sec):
#    return hour*3600 + min*60 + sec

#second = sleeptime(0,0,10)
#i = 0
#while i < 360:
#    time.sleep(second)
#    i+=1
#    #Test increase score
#    df_test['scores'] = model.decision_function(df_test[['BatteryLevel',"RSSI","Mem"]])
#    df_test['anomaly_score'] = model.predict(df_test[['BatteryLevel',"RSSI","Mem"]])
#    #df_test[df_test['anomaly_score']==-1].head(50)
#    #TestResult
#    if (list(df_test['anomaly_score']).count(-1) == 1):
#        print("anomaly value")
#    else:
#        print("ok")


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
