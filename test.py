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
INDEX_NAME = "corralert"

es = Elasticsearch('http://localhost:9200/')

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

def drawgraphictime(sensor, timearray):
	hours=[]
	value=[]
	plt.figure(sensor)
	for i in range(0,len(timearray)):
		hours.append(timearray[i][0])
		value.append(timearray[i][1])
	positionx =np.arange(len(hours))
	plt.bar(positionx,value,align="center")
	#Etiquetas xlabel
	plt.xticks(positionx,hours)
	plt.xticks(rotation=90)
	plt.xlabel("Date")
	plt.ylabel("Number of records")
	plt.title(sensor +" time")
	plt.savefig(sensor +"T.png", bbox_inches="tight", pad_inches = 0.3)
	plt.close()

def drawgrafictimetotal(fw,ids_s,radio_f,redes_mov,traf_ids,wifi, siem, bluetooth):
	hours=[]
	hoursfw=[]
	hoursids=[]
	hoursrad=[]
	hoursred=[]
	hourstraf=[]
	hourswifi=[]
	hourssiem=[]
	hoursblue=[]
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
	#Ponemos en todos los sensores elmismo numero de registros, con un 0 
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
	#Eliminamos los registros que esten dentro de los 10 primeros segundos de cada registro
	#sumamos los valores de los registros en esos 10 primeros segundos
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

	index = np.arange(len(hours2))
	plt.bar(index, valuefw, label='firewall')
	plt.bar(index, valueids, label='ids_suricata', bottom=np.array(valuefw))
	plt.bar(index, valuerad, label='radio_frec', bottom=np.array(valuefw) + np.array(valueids))
	plt.bar(index, valuered, label='redes_mov', bottom=np.array(valuefw) + np.array(valueids) + np.array(valuerad))
	plt.bar(index, valuewifi, label='wifi', bottom=np.array(valuefw) + np.array(valueids) + np.array(valuerad) + np.array(valuered))
	plt.bar(index, valuetraf, label='traf_ids', bottom=np.array(valuefw) + np.array(valueids) + np.array(valuerad) + np.array(valuered) +np.array(valuewifi))
	plt.bar(index, valuesiem, label='siem', bottom=np.array(valuefw) + np.array(valueids) + np.array(valuerad) + np.array(valuered) +np.array(valuewifi)+ np.array(valuetraf))
	plt.bar(index, valueblue, label='bluetooth', bottom=np.array(valuefw) + np.array(valueids) + np.array(valuerad) + np.array(valuered) +np.array(valuewifi)+ np.array(valuetraf) + np.array(valuesiem))
	plt.xticks(index,hours2)
	plt.xticks(rotation=90)
	plt.ylabel("Number of records")
	plt.xlabel("Date")
	plt.title('Total time')
	plt.legend()
	plt.savefig("totalTime.png", bbox_inches="tight", pad_inches = 0.3)
	plt.close()


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
	global idwifi
	#Imprime todas las horas incolucradas

#Comprobación de hipotesis nula para las horas
	pValue(hourswifi2,"wifi", hoursblue2,"bluetooth", idwifi, idblue, hours2, model)
	if t==True:
		pValue(valuewifi,"wifi", valueblue,"bluetooth", idwifi, idblue, hours2, "Time-Freq")

def pValue(sensor1,sname1, sensor2, sname2, array1, array2, hours, model):
	#Imprime las horas del primer sensor
	print(sensor1)
	#Imprime las horas del segundo sensor
	print(sensor2)
	if len(sensor1)>=2 and len(sensor2)>=2:
		hipPer = pearsonr(sensor1,sensor2)
		print(hipPer)
		#Establecemos el umbral y el intervalo de confianza
		if hipPer[0]>=umbral and hipPer[1]<= hipnull:
			print("Alert")
			alert(sname1, sname2, array1, array2, hours, "pearson", hipPer[0], model)

		hipSpe = spearmanr(sensor1,sensor2)
		print(hipSpe)
		if hipSpe[0]>=umbral and hipSpe[1]<=hipnull:
			print("Alert")
			alert(sname1, sname2, array1, array2, hours, "spearman", hipSpe[0], model)

		hipKen = kendalltau(sensor1, sensor2)
		print(hipKen)
		if hipKen[0]>=umbral and hipKen[1]<=hipnull:
			print("Alert")
			alert(sname1, sname2, array1, array2, hours, "kendall", hipKen[0], model)

#Coger los registros con prediccion 1 para el campo de la fecha de recepción de la anomalia por parte del sensor
def pred2(sensor, t):
	c = 0
	recordlist=[]
	arrayid=[]
	timelist=[]
	ind = 0
	first = True
	if sensor == "trafico_ids":
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte":time.mktime(time.localtime())-execution, "lte":time.mktime(time.localtime())}}}, {"match":{"prediction": "1"}}]}}})
	elif sensor == "ids_suricata":
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"prediction": "1"}}]}}})
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
			if isinstance(mytm,float) or isinstance(mytm,int) :
				epochDate=round(mytm)
			else:
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

	drawgraphictime(sensor,timeSort)

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
	drawgraphictime(sensor,timeSort)

def pred1ip(sensor, t, ip):
	c = 0
	recordlist=[]
	hourlist=[]
	iplist=[]
	listlist=[]
	ind = 0
	first = True
	if sensor == "trafico_ids" or sensor =="ids_suricata":
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte":time.mktime(time.localtime())-execution, "lte":time.mktime(time.localtime())}}}, {"match":{"prediction": "1"}}]}}})
	else:
		res1 = es.search(index=sensor, body={"size":10000, "query":{ "bool":{ "must": [{"range": { t:{ "gte": exe, "lte":"now"}}}, {"match":{"anomalia": True}}]}}})
	for hit in res1['hits']['hits']:
		c = c +1

		#convert same type of Epoch time
		if sensor == "ids_suricata":
			mytm = str(hit['_source'][t])
		elif sensor == "trafico_ids":
			mytm= (hit['_source'][t])
		else:
			mytma= (hit['_source'][t])
			mytm = mytma[0:19]
		fmt = ("%Y-%m-%dT%H:%M:%S")
	
		if sensor == "trafico_ids" :
			if isinstance(mytm,float) or isinstance(mytm,int):
				epochDate=round(mytm)
			else:
				h= str(mytm)
				temp = len(h)
				epochDate =int(h[:temp - 3])
			reg = str(hit['_source'][ip])
		elif sensor == "ids_suricata":
			if isinstance(mytm,float) or isinstance(mytm,int):
				epochDate=round(mytm)
			else:
				h= str(mytm)
				temp = len(h)
				epochDate =int(h[:temp - 3])
			reg = str(hit['_source']['data'][ip])
		else:
			epochDate = int(calendar.timegm(time.strptime(mytm, fmt)))
			print(epochDate)
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
	print(hoursfw)
	print(hourstraf)
	print(hoursids)
	
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
	pValue(ipidsfinal,"ids_suricata", iptraffinal,"trafico_ids", idids, idtraf, hours2, "IP")


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

def changeData(sensor, t, tr=True) :
	sources=[]
	if sensor == "ids_suricata" or sensor == "trafico_ids":
		res1 = es.search(index=sensor, body={"size":17, "query":{"match":{"prediction": "1"}}})
	if sensor == "firewall":
		res1 = es.search(index=sensor, body={"size":17, "query":{"match":{"anomalia": True}}})
	else:
		res1 = es.search(index=sensor, body={"size":8, "query":{"match":{"anomalia": True}}})
	all_reg = res1['hits']['hits']
	#los ponemos en una lista vacia
	reg = []
	for num, doc in enumerate(all_reg):
		maxnum = int(time.mktime(time.localtime()))
		if sensor=="wifi" or sensor=="bluetooth":
			minnum = int(time.mktime(time.localtime())-step)
		else:
			minnum = int(time.mktime(time.localtime())-execution)
		randnum = random.randint(minnum, maxnum)

		if sensor == "ids_suricata" or sensor == "trafico_ids":
			timestamp = random.randint(minnum, maxnum)
		else:
			timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(randnum))
		reg += [doc['_id']]

		if tr == True: #Modificamos solo el campo timestamp
			source_to_update = {
			"doc":{
			t :timestamp
			}}
		else:
			source_to_update = {
			"doc":{
			t :timestamp,
			'Source' :"17.2.19.123"
			}}

		sources.append(source_to_update)
	
	c = 0
	for num, doc_id in enumerate(reg):
		#catch API errors
		try:
			print(doc_id)
			response = es.update(index=sensor, id=doc_id, body=sources[c])
			c=c+1
		#pintamos por pantalla
			print (response, '\n\n')
			if response['result'] == "updated":
				print ("result:", response['result'])
				print ("Update was a success for ID:", response['_id'])
				print ("New data:", sources[c])
			else:
				print ("result:", response['result'])
				print ("Response failed:", response['_shards']['failed'])
		except Exception as err:
			print ("Elasticsearch Update API error:", err)

	es.indices.refresh(index=sensor)


def changeDataIP(sensor, t, s) :
	sources=[]
	if sensor == "ids_suricata" or sensor == "trafico_ids":
		res1 = es.search(index=sensor, body={"size":8, "query":{"match":{"prediction": "1"}}})
	else:
		res1 = es.search(index=sensor, body={"size":8, "query":{"match":{"anomalia": True}}})
	all_reg = res1['hits']['hits']
	#los ponemos en una lista vacia
	reg = []
	for num, doc in enumerate(all_reg):
		maxnum = int(time.mktime(time.localtime()))

		if sensor == "ids_suricata" or sensor == "trafico_ids":
			minnum = int(time.mktime(time.localtime())-step)
		else:
			minnum = int(time.mktime(time.localtime())-execution)
		randnum = random.randint(minnum, maxnum)

		if sensor == "trafico_ids" or "ids_suricata":
			timestamp = random.randint(minnum, maxnum)
		else:
			timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(randnum))
		reg += [doc['_id']]

		if sensor == "ids_suricata" or sensor == "trafico_ids":
			ip =  "192.168.0.12"
		else:
			ip = "17.2.19.123"
		#Modificamos solo el campo timestamp y la direccion ip
		if sensor == "ids_suricata":
			source_to_update = {
			"doc":{
			'data':{
				s :ip
			},
			t :timestamp}}
		else:
			source_to_update = {
			"doc":{
			t :timestamp,
			s :ip
			}}

		sources.append(source_to_update)
	
	c = 0
	for num, doc_id in enumerate(reg):
		#catch API errors
		try:
			print(doc_id)
			response = es.update(index=sensor, id=doc_id, body=sources[c])
			c=c+1
		#pintamos por pantalla
			print (response, '\n\n')
			if response['result'] == "updated":
				print ("result:", response['result'])
				print ("Update was a success for ID:", response['_id'])
				print ("New data:", sources[c])
			else:
				print ("result:", response['result'])
				print ("Response failed:", response['_shards']['failed'])
			es.indices.refresh(index=sensor)
		except Exception as err:
			print ("Elasticsearch Update API error:", err)

if len(sys.argv)<2:
	print("Error. You must introduce the number of the test.")
	print("Example: python3 test.py 1")	

elif int(sys.argv[1])==1:
	#TEST1
	changeData("bluetooth",'last_seen')
	changeData("wifi",'time')
	changeData("firewall",'Time')
	pred2("firewall",'Time')
	pred2("wifi",'time')
	pred2("bluetooth",'last_seen')
	drawgrafictimetotal(firewallT2,ids_suricataT2,radio_frecuenciaT2,redes_movilesT2,trafico_idsT2,wifiT2, siemT2, bluetoothT2)
	datosMatrix(firewallT2,ids_suricataT2,radio_frecuenciaT2,redes_movilesT2,trafico_idsT2,wifiT2, siemT2, bluetoothT2, False)
	datosMatrix(firewallT2,ids_suricataT2,radio_frecuenciaT2,redes_movilesT2,trafico_idsT2,wifiT2, siemT2, bluetoothT2, True)


elif int(sys.argv[1])==2:
	#TEST2
	changeData("firewall",'Time',False)
	changeDataIP("trafico_ids",'time_stamp','srcip')
	changeDataIP("ids_suricata",'time','src_ip')
	pred2("firewall",'Time')
	pred1("ids_suricata", 'time')
	pred2("trafico_ids",'time_stamp')
	pred1ip("firewall",'Time','Source')
	pred1ip("ids_suricata", 'time', 'src_ip')
	pred1ip("trafico_ids",'time_stamp','srcip')
	drawgrafictimetotal(firewallT2,ids_suricataT,radio_frecuenciaT2,redes_movilesT2,trafico_idsT2,wifiT2, siemT2, bluetoothT2)
	preparrayip(firewalliptime,ids_suricataiptime,trafico_idsiptime)