#!/usr/bin/python

import requests
from requests.auth import HTTPBasicAuth
import json
import sys
import mysql.connector


ENDPOINT = 'https://sps-opendata.pilotsmartke.gov.hk/rest/getCarparkInfos'

mydb = mysql.connector.connect(
  host="10.3.223.151",
  port=6033,
  user="rayson",
  passwd="P@ssw0rd",
  database="parkingdb",
  buffered= True
)


def requestData():
  try:
    response = requests.get(ENDPOINT)
    data = response.json()
    return data
  except requests.exceptions.RequestException as err:
    print(err)
    return err

update_data = ( "UPDATE parkingdb.parkinfo "
                "SET name = %s, nature = %s, motorCycle = %s, coach = %s, "
                "HGV = %s, LGV = %s, privateCar = %s, coordinates = %s "
                "WHERE parkID = %s" )

insert_data = ( "INSERT INTO parkingdb.parkinfo "
                "( parkID, name, nature, motorCycle, coach, HGV, LGV, privateCar, coordinates ) "
                "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )" )



def updateDB(dataset):
  ID = dataset["_id"]
  name = dataset["name"]
  nature = dataset["nature"]
  motorCycle = dataset["motorCycle"]["space"]
  coach = dataset["coach"]["space"]
  HGV = dataset["HGV"]["space"]
  LGV = dataset["LGV"]["space"]
  privateCar = dataset["privateCar"]["space"]
  coordinates = str(dataset["coordinates"]).strip('[]')
  sql = "SELECT * FROM parkinfo WHERE parkid = "+ str(ID)
  mycursor = mydb.cursor()
  mycursor.execute(sql)
  if mycursor.rowcount:
    data_park = (name, nature, motorCycle, coach, HGV, LGV, privateCar, coordinates, ID)
    mycursor.execute(update_data, data_park)
    mycursor.close()
  else:
    data_park = (ID, name, nature, motorCycle, coach, HGV, LGV, privateCar, coordinates)
    mycursor.execute(insert_data, data_park)
    mycursor.close()


data = requestData()["results"]
for index in range(len(data)):
  updateDB(data[index])

mydb.commit()
mydb.close()
