import requests
import json
import time
from pymongo import MongoClient
from pymongo import UpdateOne

#Importar JSON URL y guardar localmente
ext_clients = requests.get('https://62433a7fd126926d0c5d296b.mockapi.io/api/v1/usuarios')
data = ext_clients.content
with open('data.json', 'wb') as f:
    f.write(data)

#Crear Base Datos en MongoDB y cargar los datos del JSON anterior
#Crear conexion con la BD
myclient = MongoClient("mongodb://localhost:9000/")

#Crear Base de Datos en MongoDB
db = myclient["ClientesMELI"]

#Crear Coleccion en MongoDB (Multiples documentos JSON)
Collection = db["data"]

#Abrir documento JSON
with open('data.json') as file:
    file_data = json.load(file)

#Insertar los datos cargados en la coleccion
#Actualizar los datos en la BD cada 6 horas.
#Cargar en "BulkArray" para no saturar Mongo.

while(True):
    BulkArray = []
    for index, x in enumerate(file_data):
        if index <= 1:
            BulkArray.append(
                UpdateOne({"id": x['id']}, {'$set': x}, upsert=True))
        elif index > 100000 and index < 200000:
            BulkArray.append(
                UpdateOne({"id": x['id']}, {'$set': x}, upsert=True))
        else:
            BulkArray.append(
                UpdateOne({"id": x['id']}, {'$set': x}, upsert=True))
    result = Collection.bulk_write(BulkArray)
    time.sleep(21600)