import os
import numpy as np
import pandas as pd
from numpy import loadtxt
import math
from keras.models import model_from_json
from keras.models import load_model
import h5py
import psycopg2
import sqlalchemy as db


conn = db.create_engine('postgresql://postgres:postgres@localhost/test01')

try:
    connection = psycopg2.connect(user = "postgres",
                                  password = "123456",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "test01")

    cursor = connection.cursor() 

    query =  """ select "Pregnancies" , "Glucose" ,"BloodPressure" ,
                "SkinThickness" , "Insulin" , "BMI" ,
                "DiabetesPedigreeFunction","Age"
                from "Task_3".diabetes_unscored
                Except
                select "Pregnancies" , "Glucose" , "BloodPressure" ,
                "SkinThickness" , "Insulin" , "BMI" ,
                "DiabetesPedigreeFunction","Age"
                from "Task_3".diabetes_scored ;   """
    df_diabetes = pd.read_sql(query, connection)


except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
finally:
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            


df_diabetes.head(10)

df_diabetes['outcome']= ''

# load json and create model
json_file = open('model.json', 'r')
model_json = json_file.read()
json_file.close()
model = model_from_json(model_json)
# load weights into new model
model.load_weights("model.h5")
print("model loaded")



M_array = df_diabetes.iloc[:,:-1].values
predicted_Values = model.predict(M_array)


pred_bin = []
for i in predicted_Values:
    for element in i :
        if element >= 0.5:
            bin_val = 1
        else :
            bin_val = 0
        pred_bin.append(bin_val)


df_diabetes['outcome'] = pred_bin

import sqlalchemy as db

try:
    conn = db.create_engine('postgresql://postgres:123456@localhost/test01')
    df_diabetes.to_sql(name = 'diabetes_scored',schema ='Task_3',con=conn ,index = False, if_exists='append')
    print("dataframe loaded to the diabetes_scored Table")

except (Exception, psycopg2.Error) as error :
    print ("PostgreSQL connection Error", error)
finally:
        if(connection):
            cursor.close()
            connection.close()

