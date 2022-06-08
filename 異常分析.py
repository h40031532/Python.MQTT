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
    
    df2=pd.DataFrame()
    test_df=df2.append(dic,ignore_index=True)
    test_df.insert(0,column="ReceiveTime",value=receiveTime)
    
    mydata = {'receiveTime' : receiveTime, 'value' : dic}
    print(mydata)
    
    print(test_df)
   
    
    mongo_client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")
    db = mongo_client["testMongoDB"]
    db1st = mongo_client.list_database_names()
    
    mycol = db["testMongoCol"]
    collst = db.list_collection_names()
    
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



##Anomaly Detection

import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import IsolationForest
df_dict = json.loads(mycol)

#df_train = pd.DataFrame(list(df_dict), columns=['BatteryLevel',"RSSI","Mem"])
df_train = pd.DataFrame.from_dict(df_dict)
df_train.info()
df_train.head()
    
#boxplot
sns.boxplot(df_train['BatteryLevel'])

#training model
random_state = np.random.RandomState(42)
model=IsolationForest(n_estimators=100,max_samples='auto',contamination=float(0.2),random_state=random_state)
model.fit(df_train[['BatteryLevel',"RSSI","Mem"]])
print(model.get_params())

#df_test
df_dict2 = json.loads(dic)
df_test = pd.DataFrame.from_dict(df_dict2)

#time
import time

def sleeptime(hour,min,sec):
    return hour*3600 + min*60 + sec

second = sleeptime(0,0,10)
i = 0
while i < 360:
    time.sleep(second)
    i+=1
    #Increase score
    df_test['scores'] = model.decision_function(df_test[['BatteryLevel',"RSSI","Mem"]])
    df_test['anomaly_score'] = model.predict(df_test[['BatteryLevel',"RSSI","Mem"]])
    #df_test[df_train['anomaly_score']==-1].head(50)
    #Result
    if (list(df_train['anomaly_score']).count(-1) == 1):
        print("anomaly value")
    else:
        print("ok")
