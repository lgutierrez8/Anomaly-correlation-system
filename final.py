#!/usr/bin/python3

import pandas as pd
import seaborn as sns
import json
import sys
from elasticsearch import Elasticsearch
from pathlib import Path
from statistics import *
from scipy import stats
import matplotlib.pyplot as plt
import numpy as np
import time
from time import strptime, clock
import datetime
import math
import scipy.stats as st
from scipy.integrate import quad
from scipy.stats import spearmanr, pearsonr , kendalltau
from configparser import ConfigParser
import random
import calendar


config = ConfigParser()
config.read('example.conf')
step = int(config.get('GENERAL', 'step'))
umbral = float(config.get('GENERAL', 'threshold'))
hipnull = float(config.get('GENERAL', 'hipnull'))
exe ="now-"+ str(config.get('GENERAL', 'exe'))+"m"
execution = int(config.get('GENERAL', 'exe'))*60
INDEX_NAME = str(config.get('GENERAL', 'index'))

es = Elasticsearch('http://localhost:9200/')

#Crea un nuevo index si no existe:
indexcreate = False
if es.indices.exists(INDEX_NAME):
	indexcreate = True

if indexcreate == False:
# since we are running locally, use one shard and no replicas
	request_body = {
	    "settings" : {
	        "number_of_shards": 1,
	        "number_of_replicas": 0
	    },
		"mappings" : {
	    	"properties": {
	        	"timestamp": {
	            	"type": "date",
	          	}
	        }
	    }
	}
	print("creating '%s' index..." % (INDEX_NAME))
	res = es.indices.create(index = INDEX_NAME, body = request_body)
	print(" response: '%s'" % (res))


def datosMatrix(fw,ids_s,radio_f,redes_mov,traf_ids,wifi, siem, bluetooth, t):
	hours=[]
	hoursfw=[]
	hoursids=[]
	hoursrad=[]
	hoursred=[]
	hourstraf=[]
	hourswifi=[]
	hourssiem=[]
	hoursblue=[]
	hoursfw2=[]
	hoursids2=[]
	hoursrad2=[]
	hoursred2=[]
	hourstraf2=[]
	hourswifi2=[]
	hourssiem2=[]
	hoursblue2=[]
	valuefw=[]
	valueids=[]
	valuerad=[]
	valuered=[]
	valuewifi=[]
	valuetraf=[]
	valuesiem=[]
	valueblue=[]
	for i in range(0, len(fw)):
		hours.append(int(fw[i][0]))
		hoursfw.append(int(fw[i][0]))
		valuefw.append(int(fw[i][1]))
	for i in range(0, len(ids_s)):
		hours.append(int(ids_s[i][0]))
		hoursids.append(int(ids_s[i][0]))
		valueids.append(int(ids_s[i][1]))
	for i in range(0, len(radio_f)):
		hours.append(int(radio_f[i][0]))
		hoursrad.append(int(radio_f[i][0]))
		valuerad.append(int(radio_f[i][1]))
	for i in range(0, len(redes_mov)):
		hours.append(int(redes_mov[i][0]))
		hoursred.append(int(redes_mov[i][0]))
		valuered.append(int(redes_mov[i][1]))
	for i in range(0, len(traf_ids)):
		hours.append(int(traf_ids[i][0]))
		hourstraf.append(int(traf_ids[i][0]))
		valuetraf.append(int(traf_ids[i][1]))
	for i in range(0, len(wifi)):
		hours.append(wifi[i][0])
		hourswifi.append(wifi[i][0])
		valuewifi.append(wifi[i][1])
	for i in range(0, len(siem)):
		hours.append(siem[i][0])
		hourssiem.append(siem[i][0])
		valuesiem.append(siem[i][1])
	for i in range(0, len(bluetooth)):
		hours.append(bluetooth[i][0])
		hoursblue.append(bluetooth[i][0])
		valueblue.append(bluetooth[i][1])
	hours.sort()
	#Elimina los elemento duplicados
	hours2 = sorted(list(set(hours)))
	#Ponemos en todos los sensores el mismo numero de registros, con un 0 
	for i in range(0, len(hours2)):
		if hours2[i] not in hoursfw :
			valuefw.insert(i, 0)
		if hours2[i] not in hoursids :
			valueids.insert(i, 0)
		if hours2[i] not in hoursrad :
			valuerad.insert(i, 0)
		if hours2[i] not in hoursred :
			valuered.insert(i, 0)
		if hours2[i] not in hourstraf :
			valuetraf.insert(i, 0)
		if hours2[i] not in hourswifi :
			valuewifi.insert(i, 0)
		if hours2[i] not in hourssiem :
			valuesiem.insert(i, 0)
		if hours2[i] not in hoursblue :
			valueblue.insert(i, 0)
	#Eliminamos los registros que esten dentro de los X primeros segundos de cada registro
	#sumamos los valores de los registros en esos X primeros segundos
	contador = len(hours2)
	contpop = 0
	for i in range(0, len(hours2)):
		global step
		if i < contador :
			contpop = 0
			for y in range(0,len(hours2)) :
				if y < contador+contpop  and y>=i :
					if hours2[i]+step >= hours2[y - contpop] and hours2[y - contpop]!=hours2[i] :
						hours2.pop(y - contpop)
						valuefw[i]=valuefw[i]+valuefw[y - contpop]
						valuefw.pop(y - contpop)
						valueids[i]=valueids[i]+valueids[y - contpop]
						valueids.pop(y - contpop)
						valuerad[i]=valuerad[i]+valuerad[y - contpop]
						valuerad.pop(y - contpop)
						valuered[i]=valuered[i]+valuered[y - contpop]
						valuered.pop(y - contpop)
						valuetraf[i]=valuetraf[i]+valuetraf[y - contpop]
						valuetraf.pop(y - contpop)
						valuewifi[i]=valuewifi[i]+valuewifi[y - contpop]
						valuewifi.pop(y - contpop)
						valueblue[i]=valueblue[i]+valueblue[y - contpop]
						valueblue.pop(y - contpop)
						valuesiem[i]=valuesiem[i]+valuesiem[y - contpop]
						valuesiem.pop(y - contpop)
						contador = len(hours2)
						contpop = contpop + 1

	#Opción para tomar todos los registros en una misma fecha como si fuese solo 1, es decir, que existe anomalia en general en esa fecha
	if t==False:
		model = "Time-IndvAnomaly"
		for i in range(0, len(hours2)):
			if valuefw[i]>0:
				hoursfw2.append(hours2[i])
			else:
				hoursfw2.append(0)
			if valueids[i]>0:
				hoursids2.append(hours2[i])
			else:
				hoursids2.append(0)
			if valuerad[i]>0:
				hoursrad2.append(hours2[i])
			else:
				hoursrad2.append(0)
			if valuered[i]>0:
				hoursred2.append(hours2[i])
			else:
				hoursred2.append(0)
			if valuetraf[i]>0:
				hourstraf2.append(hours2[i])
			else:
				hourstraf2.append(0)
			if valuewifi[i]>0:
				hourswifi2.append(hours2[i])
			else:
				hourswifi2.append(0)
			if valuesiem[i]>0:
				hourssiem2.append(hours2[i])
			else:
				hourssiem2.append(0)
			if valueblue[i]>0:
				hoursblue2.append(hours2[i])
			else:
				hoursblue2.append(0)
	#Opcion para tener en cuenta cada uno de los registros de una fecha para realizar la correlacion
	else:
		model = "Time-NormalAnomalies"
		for i in range(0, len(hours2)):

			arr = np.array([valuefw[i], valueids[i], valuerad[i], valuered[i], valuetraf[i], valuewifi[i], valuesiem[i], valueblue[i]])
			maxval = np.amax(arr)

			if valuefw[i]>0:
				for y in range (0, valuefw[i]):
					hoursfw2.append(hours2[i])
				for y in range(0,maxval-valuefw[i]):
					hoursfw2.append(0)
			else:
				for y in range (0, maxval):
					hoursfw2.append(0)
			if valueids[i]>0:
				for y in range (0, valueids[i]):
					hoursids2.append(hours2[i])
				for y in range(0,maxval-valueids[i]):
					hoursids2.append(0)
			else:
				for y in range (0, maxval):
					hoursids2.append(0)
			if valuerad[i]>0:
				for y in range (0, valuerad[i]):
					hoursrad2.append(hours2[i])
				for y in range(0,maxval-valuerad[i]):
					hoursrad2.append(0)
			else:
				for y in range (0, maxval):
					hoursrad2.append(0)
			if valuered[i]>0:
				for y in range (0, valuered[i]):
					hoursred2.append(hours2[i])
				for y in range(0,maxval-valuered[i]):
					hoursred2.append(0)
			else:
				for y in range (0, maxval):
					hoursred2.append(0)
			if valuetraf[i]>0:
				for y in range (0, valuetraf[i]):
					hourstraf2.append(hours2[i])
				for y in range(0,maxval-valuetraf[i]):
					hourstraf2.append(0)
			else:
				for y in range (0, maxval):
					hourstraf2.append(0)
			if valuewifi[i]>0:
				for y in range (0, valuewifi[i]):
					hourswifi2.append(hours2[i])
				for y in range(0,maxval-valuewifi[i]):
					hourswifi2.append(0)
			else:
				for y in range (0, maxval):
					hourswifi2.append(0)
			if valuesiem[i]>0:
				for y in range (0, valuesiem[i]):
					hourssiem2.append(hours2[i])
				for y in range(0,maxval-valuesiem[i]):
					hourssiem2.append(0)
			else:
				for y in range (0, maxval):
					hourssiem2.append(0)
			if valueblue[i]>0:
				for y in range (0, valueblue[i]):
					hoursblue2.append(hours2[i])
				for y in range(0,maxval-valueblue[i]):
					hoursblue2.append(0)
			else:
				for y in range (0, maxval):
					hoursblue2.append(0)	

	global idfw
	global idids
	global idrad
	global idred
	global idtraf
	global idwifi
	global idblue
	global idsiem

#Comprobación de hipotesis nula para las horas
	pValue(hoursfw2,"Firewall", hoursids2,"ids_suricata", idfw, idids, hours2, model)
	pValue(hoursfw2,"Firewall", hoursrad2,"radio_frecuencia", idfw, idrad, hours2, model)
	pValue(hoursfw2,"Firewall", hoursred2,"redes_moviles", idfw, idred, hours2, model)
	pValue(hoursfw2,"Firewall", hourstraf2,"trafico_ids", idfw, idtraf, hours2, model)
	pValue(hoursfw2,"Firewall", hourswifi2,"wifi", idfw, idwifi, hours2, model)
	pValue(hoursfw2,"Firewall", hourssiem2,"siem", idfw, idsiem, hours2, model)
	pValue(hoursfw2,"Firewall", hoursblue2,"bluetooth", idfw, idblue, hours2, model)
	pValue(hoursids2,"ids_suricata", hoursrad2,"radio_frecuencia", idids, idrad, hours2, model)
	pValue(hoursids2,"ids_suricata", hoursred2,"redes_moviles", idids, idred, hours2, model)
	pValue(hoursids2,"ids_suricata", hourstraf2,"trafico_ids", idids, idtraf, hours2, model)
	pValue(hoursids2,"ids_suricata", hourswifi2,"wifi", idids, idwifi, hours2, model)
	pValue(hoursids2,"ids_suricata", hourssiem2,"siem", idids, idsiem, hours2, model)
	pValue(hoursids2,"ids_suricata", hoursblue2,"bluetooth", idids, idblue, hours2, model)
	pValue(hoursrad2,"radio_frecuencia", hoursred2,"redes_moviles", idrad, idred, hours2, model)
	pValue(hoursrad2,"radio_frecuencia", hourstraf2,"trafico_ids", idrad, idtraf, hours2, model)
	pValue(hoursrad2,"radio_frecuencia", hourswifi2,"wifi", idrad, idwifi, hours2, model)
	pValue(hoursrad2,"radio_frecuencia", hourssiem2,"siem", idrad, idsiem, hours2, model)
	pValue(hoursrad2,"radio_frecuencia", hoursblue2,"bluetooth", idrad, idblue, hours2, model)
	pValue(hoursred2,"redes_moviles", hourstraf2,"trafico_ids", idred, idtraf, hours2, model)
	pValue(hoursred2,"redes_moviles", hourswifi2,"wifi", idred, idwifi, hours2, model)
	pValue(hoursred2,"redes_moviles", hourssiem2,"siem", idred, idsiem, hours2, model)
	pValue(hoursred2,"redes_moviles", hoursblue2,"bluetooth", idred, idblue, hours2, model)
	pValue(hourstraf2,"trafico_ids", hourswifi2,"wifi", idtraf, idwifi, hours2, model)
	pValue(hourstraf2,"trafico_ids", hourssiem2,"siem", idtraf, idsiem, hours2, model)
	pValue(hourstraf2,"trafico_ids", hoursblue2,"bluetooth", idtraf, idblue, hours2, model)
	pValue(hourswifi2,"wifi", hourssiem2,"siem", idwifi, idsiem, hours2, model)
	pValue(hourswifi2,"wifi", hoursblue2,"bluetooth", idwifi, idblue, hours2, model)
	pValue(hourssiem2,"siem", hoursblue2,"bluetooth", idsiem, idblue, hours2, model)
	
	
#Comprobación de hipotesis nula para las frecuencias
	if t==True:
		pValue(valuefw,"Firewall", valueids,"ids_suricata", idfw, idids, hours2, "Time-Freq")
		pValue(valuefw,"Firewall", valuerad,"radio_frecuencia", idfw, idrad, hours2, "Time-Freq")
		pValue(valuefw,"Firewall", valuered,"redes_moviles", idfw, idred, hours2, "Time-Freq")
		pValue(valuefw,"Firewall", valuetraf,"trafico_ids", idfw, idtraf, hours, "Time-Freq")
		pValue(valuefw,"Firewall", valuewifi,"wifi", idfw, idwifi, hours2, "Time-Freq")
		pValue(valuefw,"Firewall", valuesiem,"siem", idfw, idsiem, hours2, "Time-Freq")
		pValue(valuefw,"Firewall", valueblue,"bluetooth", idfw, idblue, hours2, "Time-Freq")
		pValue(valueids,"ids_suricata", valuerad,"radio_frecuencia", idids, idrad, hours2, "Time-Freq")
		pValue(valueids,"ids_suricata", valuered,"redes_moviles", idids, idred, hours2, "Time-Freq")
		pValue(valueids,"ids_suricata", valuetraf,"trafico_ids", idids, idtraf, hours2, "Time-Freq")
		pValue(valueids,"ids_suricata", valuewifi,"wifi", idids, idwifi, hours2, "Time-Freq")
		pValue(valueids,"ids_suricata", valuesiem,"siem", idids, idsiem, hours2, "Time-Freq")
		pValue(valueids,"ids_suricata", valueblue,"bluetooth", idids, idblue, hours2, "Time-Freq")
		pValue(valuerad,"radio_frecuencia", valuered,"redes_moviles", idrad, idred, hours2, "Time-Freq")
		pValue(valuerad,"radio_frecuencia", valuetraf,"trafico_ids", idrad, idtraf, hours2, "Time-Freq")
		pValue(valuerad,"radio_frecuencia", valuewifi,"wifi", idrad, idwifi, hours2, "Time-Freq")
		pValue(valuerad,"radio_frecuencia", valuesiem,"siem", idrad, idsiem, hours2, "Time-Freq")
		pValue(valuerad,"radio_frecuencia", valueblue,"bluetooth", idrad, idblue, hours2, "Time-Freq")
		pValue(valuered,"redes_moviles", valuetraf,"trafico_ids", idred, idtraf, hours2, "Time-Freq")
		pValue(valuered,"redes_moviles", valuewifi,"wifi", idred, idwifi, hours2, "Time-Freq")
		pValue(valuered,"redes_moviles", valuesiem,"siem", idred, idsiem, hours2, "Time-Freq")
		pValue(valuered,"redes_moviles", valueblue,"bluetooth", idred, idblue, hours2, "Time-Freq")
		pValue(valuetraf,"trafico_ids", valuewifi,"wifi", idtraf, idwifi, hours2, "Time-Freq")
		pValue(valuetraf,"trafico_ids", valuesiem,"siem", idtraf, idsiem, hours2, "Time-Freq")
		pValue(valuetraf,"trafico_ids", valueblue,"bluetooth", idtraf, idblue, hours2, "Time-Freq")
		pValue(valuewifi,"wifi", valuesiem,"siem", idwifi, idsiem, hours2, "Time-Freq")
		pValue(valuewifi,"wifi", valueblue,"bluetooth", idwifi, idblue, hours2, "Time-Freq")
		pValue(valuesiem,"siem", valueblue,"bluetooth", idsiem, idblue, hours2, "Time-Freq")
	
	
def pValue(sensor1,sname1, sensor2, sname2, array1, array2, hours, model):
	#La longitud debe ser mayor de 2 ya que sino no se puede calcular la correlacion
	if len(sensor1)>=2 and len(sensor2)>=2:

		if model = "Time-Freq":
			hipCorr = spearmanr(sensor1,sensor2)
			if hipCorr[0]>=umbral and hipCorr[1]<= hipnull:
				alert(sname1, sname2, array1, array2, hours, "spearman", hipCorr[0], model)
		else:
			hipCorr = pearsonr(sensor1,sensor2)
			if hipCorr[0]>=umbral and hipCorr[1]<= hipnull:
				alert(sname1, sname2, array1, array2, hours, "pearson", hipCorr[0], model)

		

#Coger los registros con prediccion 1 para el campo de envio de datos a la base de datos
def pred1(sensor, t):
	c = 0
	recordlist=[]
	timelist=[]
	ind = 0
	first = True
	if sensor == "ids_suricata" or sensor == "trafico_ids":
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte":time.mktime(time.localtime())-execution, "lte":time.mktime(time.localtime())}}}, {"match":{"prediction": "1"}}]}}})
	else:
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"anomalia": True}}]}}})
	for hit in res1['hits']['hits']:
		c = c +1
		#convert same type of Epoch time
		if sensor == "ids_suricata" or sensor == "trafico_ids":
			mytm = (hit['_source'][t])
		else:
			mytma= (hit['_source'][t])
			mytm = mytma[0:19]
		fmt = ("%Y-%m-%dT%H:%M:%S")
		if sensor == "ids_suricata":
			epochDate= (math.floor(mytm))
		elif sensor == "trafico_ids":
			h= str(mytm)
			temp = len(h)
			epochDate =int(h[:temp - 3])
		else:
			epochDate = int(calendar.timegm(time.strptime(mytm, fmt)))

		if sensor == "firewall":
			global firewallTs
			firewallTs.append(int(epochDate))
			firewallTs.sort()
		if sensor == "ids_suricata":
			global ids_suricataTs
			ids_suricataTs.append(int(epochDate))
			ids_suricataTs.sort()
		if sensor == "radio_frecuencia":
			global radio_frecuenciaTs
			radio_frecuenciaTs.append(int(epochDate))
			radio_frecuenciaTs.sort()
		if sensor == "redes_moviles":
			global redes_movilesTs
			redes_movilesTs.append(int(epochDate))
			redes_movilesTs.sort()
		if sensor == "trafico_ids":
			global trafico_idsTs
			trafico_idsTs.append(int(epochDate))
			trafico_idsTs.sort()
		if sensor == "wifi":
			global wifiTs
			wifiTs.append(int(epochDate))
			wifiTs.sort()
		if sensor == "bluetooth":
			global bluetoothTs
			bluetoothTs.append(int(epochDate))
			bluetoothTs.sort()
		if sensor == "siem":
			global siemTs
			siemTs.append(int(epochDate))
			siemTs.sort()

		#First record
		if first:
			recordlist.append(epochDate)
			lista=[epochDate, 1]
			timelist.append(lista)
			first=False
		#Calculate the frequecy of the hours for each record
		else:
			if epochDate in recordlist:
				ind = recordlist.index(epochDate)
				timelist[ind][1]=timelist[ind][1]+1
			else:
				lista=[epochDate, 1]
				timelist.append(lista)
				recordlist.append(epochDate)

	timeSort = sorted(timelist, key= lambda time : time[0])
	if sensor == "firewall":
		global firewallT
		firewallT = timeSort
	if sensor == "ids_suricata":
		global ids_suricataT
		ids_suricataT = timeSort
	if sensor == "radio_frecuencia":
		global radio_frecuenciaT
		radio_frecuenciaT = timeSort
	if sensor == "redes_moviles":
		global redes_movilesT
		redes_movilesT = timeSort
	if sensor == "trafico_ids":
		global trafico_idsT
		trafico_idsT = timeSort
	if sensor == "wifi":
		global wifiT
		wifiT = timeSort
	if sensor == "bluetooth":
		global bluetoothT
		bluetoothT = timeSort
	if sensor == "siem":
		global siemT
		siemT = timeSort



#Coger los registros con prediccion 1 para el campo de la fecha de recepción de la anomalia por parte del sensor
def pred2(sensor, t):
	c = 0
	recordlist=[]
	arrayid=[]
	timelist=[]
	ind = 0
	first = True
	if sensor == "ids_suricata" or sensor == "trafico_ids":
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte":time.mktime(time.localtime())-execution, "lte":time.mktime(time.localtime())}}}, {"match":{"prediction": "1"}}]}}})
	else:
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"anomalia": True}}]}}})
	for hit in res1['hits']['hits']:
		c = c +1
		#convert same type of Epoch time
		if sensor == "ids_suricata":
			mytma = str(hit['_source']['data'][t])
			mytm = mytma[0:19]
		elif sensor == "trafico_ids":
			mytm= (hit['_source'][t])
		else:
			mytma= (hit['_source'][t])
			mytm = mytma[0:19]

		fmt = ("%Y-%m-%dT%H:%M:%S")
	
		if sensor == "trafico_ids":
			h= str(mytm)
			temp = len(h)
			epochDate =int(h[:temp - 3])
		else:
			epochDate = int(calendar.timegm(time.strptime(mytm, fmt)))
		
		if sensor == "firewall":
			global firewallTim
			firewallTim.append(int(epochDate))
			firewallTim.sort()
		if sensor == "ids_suricata":
			global ids_suricataTim
			ids_suricataTim.append(int(epochDate))
			ids_suricataTim.sort()
		if sensor == "radio_frecuencia":
			global radio_frecuenciaTim
			radio_frecuenciaTim.append(int(epochDate))
			radio_frecuenciaTim.sort()
		if sensor == "redes_moviles":
			global redes_movilesTim
			redes_movilesTim.append(int(epochDate))
			redes_movilesTim.sort()
		if sensor == "trafico_ids":
			global trafico_idsTim
			trafico_idsTim.append(int(epochDate))
			trafico_idsTim.sort()
		if sensor == "wifi":
			global wifiTim
			wifiTim.append(int(epochDate))
			wifiTim.sort()
		if sensor == "bluetooth":
			global bluetoothTim
			bluetoothTim.append(int(epochDate))
			bluetoothTim.sort()
		if sensor == "siem":
			global siemTim
			siemTim.append(int(epochDate))
			siemTim.sort()

		#Array para tener los ids con la fecha
		objid=[epochDate,hit['_id']]
		arrayid.append(objid)

		#First record
		if first:
			recordlist.append(epochDate)
			lista=[epochDate, 1]
			timelist.append(lista)
			first=False
		#Calculate the frequecy of the hours for each record
		else:
			if epochDate in recordlist:
				ind = recordlist.index(epochDate)
				timelist[ind][1]=timelist[ind][1]+1
			else:
				lista=[epochDate, 1]
				timelist.append(lista)
				recordlist.append(epochDate)

	timeSort = sorted(timelist, key= lambda time : time[0])
	if sensor == "firewall":
		global firewallT2
		global idfw
		idfw= arrayid
		firewallT2 = timeSort
	if sensor == "ids_suricata":
		global ids_suricataT2
		global idids
		idids= arrayid
		ids_suricataT2 = timeSort
	if sensor == "radio_frecuencia":
		global radio_frecuenciaT2
		global idrad
		idrad= arrayid
		radio_frecuenciaT2 = timeSort
	if sensor == "redes_moviles":
		global redes_movilesT2
		global idred
		idred= arrayid
		redes_movilesT2 = timeSort
	if sensor == "trafico_ids":
		global trafico_idsT2
		global idtraf
		idtraf= arrayid
		trafico_idsT2 = timeSort
	if sensor == "wifi":
		global wifiT2
		global idwifi
		idwifi= arrayid
		wifiT2 = timeSort
	if sensor == "bluetooth":
		global bluetoothT2
		global idblue
		idblue= arrayid
		bluetoothT2 = timeSort
	if sensor == "siem":
		global siemT2
		global idsiem
		idsiem= arrayid
		siemT2 = timeSort



def pred1ip(sensor, t, ip):
	c = 0
	recordlist=[]
	hourlist=[]
	iplist=[]
	listlist=[]
	ind = 0
	first = True
	if sensor == "ids_suricata" or sensor == "trafico_ids":
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte":time.mktime(time.localtime())-execution, "lte":time.mktime(time.localtime())}}}, {"match":{"prediction": "1"}}]}}})
	else:
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"anomalia": True}}]}}})
	for hit in res1['hits']['hits']:
		c = c +1

		#convert same type of Epoch time
		if sensor == "ids_suricata":
			mytma = str(hit['_source']['data'][t])
			mytm = mytma[0:19]
		elif sensor == "trafico_ids":
			mytm= (hit['_source'][t])
		else:
			mytma= (hit['_source'][t])
			mytm = mytma[0:19]
		fmt = ("%Y-%m-%dT%H:%M:%S")
	
		if sensor == "trafico_ids":
			h= str(mytm)
			temp = len(h)
			epochDate =int(h[:temp - 3])
			reg = str(hit['_source'][ip])
		elif sensor == "ids_suricata":
			epochDate = int(calendar.timegm(time.strptime(mytm, fmt)))
			if str(hit['_source']['data']['event_type'])!="stats":
				reg = str(hit['_source']['data'][ip])
		else:
			epochDate = int(calendar.timegm(time.strptime(mytm, fmt)))
			reg = str(hit['_source'][ip])
		
		val=[epochDate, reg]

		#First record
		if first:
			lista=[epochDate, reg, 1]
			iplist.append(lista)
			listlist.append(val)
			first=False
		#Calculate the frequecy of the hours for each record
		else:
			if val in listlist:
				ind = listlist.index(val)
				iplist[ind][2]=iplist[ind][2]+1
			else:
				lista=[epochDate, reg, 1]
				iplist.append(lista)
				listlist.append(val)
	timeSort = sorted(iplist, key= lambda time : time[0])

	if sensor == "firewall":
		global firewalliptime
		firewalliptime = timeSort
	if sensor == "ids_suricata":
		global ids_suricataiptime
		ids_suricataiptime = timeSort
	if sensor == "radio_frecuencia":
		global radio_frecuenciaiptime
		radio_frecuenciaiptime = timeSort
	if sensor == "redes_moviles":
		global redes_movilesiptime
		redes_movilesiptime = timeSort
	if sensor == "trafico_ids":
		global trafico_idsiptime
		trafico_idsiptime = timeSort
	if sensor == "wifi":
		global wifiiptime
		wifiiptime = timeSort
	if sensor == "bluetooth":
		global bluetoothiptime
		bluetoothiptime = timeSort
	if sensor == "siem":
		global siemiptime
		siemiptime = timeSort


def preparrayip(fw,ids_s,traf_ids):
	fwip=[]
	idsip=[]
	trafip=[]
	freqhfw=[]
	freqhids=[]
	freqhtraf=[]
	hoursfw=[]
	hoursids=[]
	hourstraf=[]
	for i in range(0,len(fw)):
		if fw[i][0] not in hoursfw:
			hoursfw.append(fw[i][0])
			freqhfw.append(fw[i][2])
		else:
			freqhfw[len(freqhfw)-1]=freqhfw[len(freqhfw)-1]+fw[i][2]
	for i in range(0,len(ids_s)):
		if ids_s[i][0] not in hoursids:
			hoursids.append(ids_s[i][0])
			freqhids.append(ids_s[i][2])
		else:
			freqhids[len(freqhids)-1]=freqhids[len(freqhids)-1]+ids_s[i][2]
	for i in range(0,len(traf_ids)):
		if traf_ids[i][0] not in hourstraf:
			hourstraf.append(traf_ids[i][0])
			freqhtraf.append(traf_ids[i][2])
		else:
			freqhtraf[len(freqhtraf)-1]=freqhtraf[len(freqhtraf)-1]+traf_ids[i][2]
	
	arrayip(fw,freqhfw,hoursfw,ids_s,freqhids,hoursids,traf_ids,freqhtraf,hourstraf)

def arrayip(fw,freqhfw,hoursfw,ids_s,freqhids,hoursids,traf_ids,freqhtraf,hourstraf):
	hours=[]
	hoursfw2=[]
	hoursids2=[]
	hourstraf2=[]
	ipsfw=[]
	ipsids=[]
	ipstraf=[]
	ipfw=[]
	ipids=[]
	iptraf=[]
	#Crea un array con todaslas direcciones ip en orden
	for i in range(0,len(fw)):
		for y in range(0,fw[i][2]):
			ipsfw.append(fw[i][1])
	for i in range(0,len(ids_s)):
		for y in range(0,ids_s[i][2]):
			ipsids.append(ids_s[i][1])
	for i in range(0,len(traf_ids)):
		for y in range(0,traf_ids[i][2]):
			ipstraf.append(traf_ids[i][1])
	#Crea un array con las horas de todos los dispositivos
	for i in range(0, len(hoursfw)):
		hours.append(int(hoursfw[i]))
	for i in range(0, len(hoursids)):
		hours.append(int(hoursids[i]))
	for i in range(0, len(hourstraf)):
		hours.append(int(hourstraf[i]))
	hours.sort()
	#Elimina los elemento duplicados
	hours2 = sorted(list(set(hours)))
	#Ponemos en todos los sensores el mismo numero de registros, con un 0 
	for i in range(0, len(hours2)):
		if hours2[i] not in hoursfw :
			freqhfw.insert(i, 0)
		if hours2[i] not in hoursids :
			freqhids.insert(i, 0)
		if hours2[i] not in hourstraf :
			freqhtraf.insert(i, 0)

	#Eliminamos los registros que esten dentro de los x primeros segundos de cada registro
	#sumamos los valores de los registros en esos x primeros segundos
	contador = len(hours2)
	contpop = 0
	for i in range(0, len(hours2)):
		global step
		if i < contador :
			contpop = 0
			for y in range(0,len(hours2)) :
				if y < contador+contpop  and y>=i :
					if hours2[i]+step >= hours2[y - contpop] and hours2[y - contpop]!=hours2[i] :
						hours2.pop(y - contpop)
						freqhfw[i]=freqhfw[i]+freqhfw[y - contpop]
						freqhfw.pop(y - contpop)
						freqhids[i]=freqhids[i]+freqhids[y - contpop]
						freqhids.pop(y - contpop)
						freqhtraf[i]=freqhtraf[i]+freqhtraf[y - contpop]
						freqhtraf.pop(y - contpop)
						contador = len(hours2)
						contpop = contpop + 1

	#Metemos las horas el nº deveces que indique la maxima frecuencia y en su caso ceros.
	for i in range(0, len(hours2)):

		arr = np.array([freqhfw[i], freqhids[i],  freqhtraf[i]])
		maxval = np.amax(arr)

		if freqhfw[i]>0:
			for y in range (0, freqhfw[i]):
				hoursfw2.append(hours2[i])
			for y in range(0,maxval-freqhfw[i]):
				hoursfw2.append(0)
		else:
			for y in range (0, maxval):
				hoursfw2.append(0)
		if freqhids[i]>0:
			for y in range (0, freqhids[i]):
				hoursids2.append(hours2[i])
			for y in range(0,maxval-freqhids[i]):
				hoursids2.append(0)
		else:
			for y in range (0, maxval):
				hoursids2.append(0)
		if freqhtraf[i]>0:
			for y in range (0, freqhtraf[i]):
				hourstraf2.append(hours2[i])
			for y in range(0,maxval-freqhtraf[i]):
				hourstraf2.append(0)
		else:
			for y in range (0, maxval):
				hourstraf2.append(0)

	#Metemos las direcciones IP en las posiciones donde hay horas (por orden)
	contadorfw=0		
	for i in range(0, len(hoursfw2)):
		if hoursfw2[i]!=0:
			ipfw.append(ipsfw[contadorfw])
			contadorfw=contadorfw+1
		else:
			ipfw.append(0)
	contadorids=0		
	for i in range(0, len(hoursids2)):
		if hoursids2[i]!=0:
			ipids.append(ipsids[contadorids])
			contadorids=contadorids+1
		else:
			ipids.append(0)
	contadortraf=0		
	for i in range(0, len(hourstraf2)):
		if hourstraf2[i]!=0:
			iptraf.append(ipstraf[contadortraf])
			contadortraf=contadortraf+1
		else:
			iptraf.append(0)

	#Quitamos los espacios en blanco, las palabras y los puntos
	ipfw2=[]
	for i in range(0, len(ipfw)):
		new = str(ipfw[i])
		first = new.find("(")
		second = new.rfind(")")
		if (first != -1) and (second != -1):
			new = new[first+1:second]
		ipfw2.append(new)
	ipfwfinal=[]
	ipidsfinal=[]
	iptraffinal=[]
	for i in range(0, len(ipfw2)):
		new = str(ipfw2[i]).replace('.','')
		ipfwfinal.append(new)
	for i in range(0, len(ipids)):
		new = str(ipids[i]).replace('.','')
		ipidsfinal.append(new)
	for i in range(0, len(iptraf)):
		new = str(iptraf[i]).replace('.','')
		iptraffinal.append(new)
	#Lo pasamos todo a int
	for i in range(0, len(ipfwfinal)):
		ipfwfinal[i]=int(ipfwfinal[i])
		ipidsfinal[i]=int(ipidsfinal[i])
		iptraffinal[i]=int(iptraffinal[i])

	global idids
	global idtraf
	global idfw
	
	#Comprobacion hipotesis no nula:
	pValue(ipfwfinal,"Firewall", ipidsfinal,"ids_suricata", idfw, idids, hours2, "IP")
	pValue(ipfwfinal,"Firewall", iptraffinal,"trafico_ids", idfw, idtraf, hours2, "IP")
	pValue(iptraffinal,"trafico_ids", ipidsfinal,"ids_suricata", idtraf, idids, hours2, "IP")


def ip(sensor, ip):
	global execution
	recordlist=[]
	hourlist=[]
	iplist=[]
	listlist=[]
	ind = 0
	first = True
	if sensor == "ids_suricata" or sensor == "trafico_ids":
			res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte":time.mktime(time.localtime())-execution, "lte":time.mktime(time.localtime())}}}, {"match":{"prediction": "1"}}]}}})
	else:
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"anomalia": True}}]}}})
	for hit in res1['hits']['hits']:

		if sensor == "ids_suricata":
			if str(hit['_source']['data']['event_type'])!="stats":
				reg = str(hit['_source']['data'][ip])
		else:
			reg = str(hit['_source'][ip])
		#Add the different IP
		if reg not in iplist:
			iplist.append(reg)		

	if sensor == "firewall":
		global firewallip
		firewallip = iplist
	if sensor == "ids_suricata":
		global ids_suricataip
		ids_suricataip = iplist
	if sensor == "trafico_ids":
		global trafico_idsip
		trafico_idsip = iplist

def mac(sensor, mac):
	recordlist=[]
	iplist=[]
	listlist=[]
	ind = 0
	first = True
	res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"anomalia": True}}]}}})
	for hit in res1['hits']['hits']:
		reg = str(hit['_source'][mac])
		#Add the different IP
		if reg not in iplist:
			iplist.append(reg)		

	if sensor == "bluetooth":
		global bluetoothip
		bluetoothip = iplist
	if sensor == "wifi":
		global wifiip
		wifiip = iplist
	
#Compara si existe alguna ip igual
def comparatorip(firewall, suricata, trafico):
	iplistfirtraf=[]
	iplistfirsur=[]
	iplistsurtraf=[]
	print(trafico_idsip)
	print(firewallip)
	print(trafico_idsip)
	for i in range(0, len(firewall)):
		if firewall[i] in trafico:
			iplistfirtraf.append(firewall[i])
	for i in range(0, len(suricata)):
		if suricata[i] in trafico:
			iplistsurtraf.append(suricata[i])
	for i in range(0, len(firewall)):
		if firewall[i] in suricata:
			iplistfirsur.append(firewall[i])
	print(len(iplistfirtraf))
	print(len(iplistsurtraf))
	print(len(iplistfirsur))

#Compara si existe alguna mac igual
def comparatormac(bluetooth, wifi):
	iplistbluewif=[]
	print(bluetooth)
	print(wifi)
	for i in range(0, len(bluetooth)):
		if bluetooth[i] in wifi:
			iplistbluewif.append(bluetooth[i])
	print(len(iplistbluewif))
	

#Creación de la alerta
def alert(sensor1, sensor2, arrayid1,arrayid2, hours, corrType, corrVal, model):
	global step
	global hipnull
	global umbral
	ids1=[]
	ids2=[]
	for y in range(0, len(hours)):
		for i in range(0, len(arrayid1)):
			if hours[y] <= arrayid1[i][0] and hours[y]+step>= arrayid1[i][0]:
				ids1.append(arrayid1[i][1])
		for i in range(0, len(arrayid2)):
			if hours[y] <= arrayid2[i][0] and hours[y]+step>= arrayid2[i][0]:
				ids2.append(arrayid2[i][1])	

	print(hours)
	time_start = time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime(hours[0]))
	time_final = time.strftime('%Y-%m-%dT%H:%M:%S+00:00', time.gmtime(hours[len(hours)-1]+step))

	timestamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

	alertc= {}
	alertc['timestamp'] = timestamp
	alertc['type'] = model
	alertc['CorrType'] = corrType
	alertc['CorrVal'] = corrVal
	alertc['IntitHour'] = time_start
	alertc['FinalHour'] = time_final
	alertc['HipNull'] = hipnull
	alertc['Umbral'] = umbral
	alertc['TimeStep'] = step
	alertc['sensors'] = [0,0]
	alertc['sensors'][0] = []
	alertc['sensors'][1] = []
	alertc['sensors'][0].append({
			'Name':sensor1,
			'NumRecords':str(len(ids1))})
	alertc['sensors'][1].append({
			'Name':sensor2,
			'NumRecords':str(len(ids2))})
	for i in range(0,len(ids1)):
		alertc['sensors'][0].append({
			'id':str(ids1[i])})			
	for i in range(0,len(ids2)):
		alertc['sensors'][1].append({
			'id':ids2[i]})
	
	with open('alert.json', 'w') as file:
		json.dump(alertc, file, indent=4)

#subimos la alerta
	f = open("alert.json")
	docket_content = f.read()
	res = es.index(index="corralert", body=json.loads(docket_content), refresh = True)
	print(res['result'])
	es.indices.refresh(index="corralert")

while True:
	starttime = time.time()  

	idfw=[]
	idids=[]
	idrad=[]
	idred=[]
	idtraf=[]
	idwifi=[]
	idsiem=[]
	idblue=[]

	firewallT=[]
	ids_suricataT=[]
	radio_frecuenciaT=[]
	redes_movilesT=[]
	trafico_idsT=[]
	wifiT=[]
	siemT=[]
	bluetoothT=[]

	firewallT2=[]
	ids_suricataT2=[]
	radio_frecuenciaT2=[]
	redes_movilesT2=[]
	trafico_idsT2=[]
	wifiT2=[]
	siemT2=[]
	bluetoothT2=[]

	firewallTs=[]
	ids_suricataTs=[]
	radio_frecuenciaTs=[]
	redes_movilesTs=[]
	trafico_idsTs=[]
	wifiTs=[]
	siemTs=[]
	bluetoothTs=[]

	firewallTim=[]
	ids_suricataTim=[]
	radio_frecuenciaTim=[]
	redes_movilesTim=[]
	trafico_idsTim=[]
	wifiTim=[]
	siemTim=[]
	bluetoothTim=[]

	firewallip=[]
	ids_suricataip=[]
	radio_frecuenciaip=[]
	redes_movilesip=[]
	trafico_idsip=[]
	wifiip=[]
	siemip=[]
	bluetoothip=[]

	firewalliptime=[]
	ids_suricataiptime=[]
	radio_frecuenciaiptime=[]
	redes_movilesiptime=[]
	trafico_idsiptime=[]
	wifiiptime=[]
	bluetoothtime=[]
	siemtime=[]

	s_alert=[]
	s_http=[]
	s_stats=[]
	s_fileinfo=[]
	s_flow=[]

	#Para sacar las correlaciones de los tiempo de recepcion:
	pred2("firewall",'Time')
	pred2("ids_suricata", 'timestamp')
	pred2("radio_frecuencia", 'time')
	pred2("redes_moviles", 'time')
	pred2("trafico_ids",'time_stamp')
	pred2("wifi",'time')
	pred2("bluetooth",'last_seen')
	pred2("siem",'Date')
	datosMatrix(firewallT2,ids_suricataT2,radio_frecuenciaT2,redes_movilesT2,trafico_idsT2,wifiT2, siemT2, bluetoothT2, True)
	datosMatrix(firewallT2,ids_suricataT2,radio_frecuenciaT2,redes_movilesT2,trafico_idsT2,wifiT2, siemT2, bluetoothT2, False)

	#Para sacar las correlaciones de los tiempos de envio a la base de datos: (False es para tratar los registros en un mismo rango de tiempo como uno unico)
	pred1("firewall",'timestamp')
	pred1("ids_suricata", 'time')
	pred1("radio_frecuencia", 'timestamp')
	pred1("redes_moviles", 'timestamp')
	pred1("trafico_ids",'time_stamp')
	pred1("wifi",'timestamp')
	pred1("bluetooth",'timestamp')
	pred1("siem",'timestamp')
	datosMatrix(firewallT,ids_suricataT,radio_frecuenciaT,redes_movilesT,trafico_idsT,wifiT, siemT, bluetoothT, True)
	datosMatrix(firewallT,ids_suricataT,radio_frecuenciaT,redes_movilesT,trafico_idsT,wifiT, siemT, bluetoothT, False)


	#Para comprobar si hay alguna direccion MAC igual
	#pred1ip("wifi",'time', 'userid')
	#pred1ip("bluetooth",'last_seen', 'address')
	#mac("wifi", 'userid')
	#mac("bluetooth", 'address')
	#comparatormac(bluetoothip, wifiip)


	#Para comparar si hay alguna direccion IP igual
	#pred1ip("firewall",'Time','Source')
	#pred1ip("ids_suricata", 'timestamp', 'src_ip')
	#pred1ip("trafico_ids",'time_stamp','srcip')
	#ip("firewall",'Source')
	#ip("ids_suricata",'src_ip')
	#ip("trafico_ids",'srcip')
	#comparatorip(firewallip,ids_suricataip,trafico_idsip)


	#Para calcular la correlacion de las direcciones IP
	pred1ip("firewall",'Time','Source')
	pred1ip("ids_suricata", 'timestamp', 'src_ip')
	pred1ip("trafico_ids",'time_stamp','srcip')
	preparrayip(firewalliptime,ids_suricataiptime,trafico_idsiptime)

	#SE EJECUTA CADA 15MIN
	time.sleep(execution - ((time.time() - starttime)))