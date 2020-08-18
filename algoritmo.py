#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#
# Script to download the modis file from NASA repository
# Author: Jorge Mauricio
# Email: jorge.ernesto.mauricio@gmail.com
# Date: Created on Thu Sep 28 08:38:15 2017
# Version: 1.0
#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
"""

# librerías
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import urllib.request
import os
import time
import re
import datetime
import pyodbc
from pyhdf.SD import SD, SDC
from access import usr, pwd, server, database, usr_db, pwd_db

def main():

    links = []

    class SessionWithHeaderRedirection(requests.Session):

        AUTH_HOST = 'urs.earthdata.nasa.gov'

        def __init__(self, username, password):

            super().__init__()

            self.auth = (username, password)


       # Overrides from the library to keep headers when redirected to or from
       # the NASA auth host.

        def rebuild_auth(self, prepared_request, response):

            headers = prepared_request.headers

            url = prepared_request.url



            if 'Authorization' in headers:

                original_parsed = requests.utils.urlparse(response.request.url)

                redirect_parsed = requests.utils.urlparse(url)

                if (original_parsed.hostname != redirect_parsed.hostname) and redirect_parsed.hostname != self.AUTH_HOST and original_parsed.hostname != self.AUTH_HOST:

                    del headers['Authorization']

            return



    # create session with the user credentials that will be used to authenticate access to the data
    session = SessionWithHeaderRedirection(usr, pwd)

    # the url of the file we wish to retrieve
    url = "https://e4ftl01.cr.usgs.gov/MOLA/MYD13C1.006/"

    # extract the filename from the url to be used when saving the file
    # filename = url[url.rfind('/')+1:]

    try:

        # read last_file donwloaded
        f = open("last_file.txt", "r")

        last_file = f.read()

        # submit the request using the session
        response = session.get(url, stream=True)

        # print(response.text)

        # parsear la información
        soup = BeautifulSoup(response.text, "html.parser")

        # guardar links de subcarpetas
        array_subcarpetas = []

        # ciclo de parseo
        for link in soup.find_all("a"):
            array_subcarpetas.append(link.get("href"))

        # obtener los archivos individuales
        subfolder = array_subcarpetas[-1]

        #for subfolder in array_subcarpetas[7:]:
        # crear url de subcarpeta
        SUB_URL = "{}{}".format(url,subfolder)

        # consulta subcarpeta
        # print(SUB_URL)
        r_subfolder = requests.get(SUB_URL)

        # parser información
        soup_subfolder = BeautifulSoup(r_subfolder.text, "html.parser")

        array_archivos = []

        for link_carpetas in soup_subfolder.find_all("a"):
            array_archivos.append(link_carpetas.get("href"))

        # nombre del archivo
        nombre_archivo = array_archivos[-2]

        # url para descarga
        URL_DESCARGA = "{}{}{}".format(url,subfolder,nombre_archivo)

        # descarga del archivo
        if URL_DESCARGA.endswith(".hdf"):

            print(URL_DESCARGA)

            # add link to array
            links.append(URL_DESCARGA)

        if last_file.strip() == URL_DESCARGA.strip():
            # print status
            print("no update")

        else:

            print("* * * ", last_file)
            print("* * * ", URL_DESCARGA)
            # download file
            os.system("wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --keep-session-cookies {}".format(URL_DESCARGA))

            text_file = open("last_file.txt", "w")

            text_file.write(URL_DESCARGA)

            text_file.close()

            # move the downloaded file
            os.system("mv {} /home/jorge/Documents/Research/get_modis_files_from_nasa/data/".format(nombre_archivo))

            # run evi
            generar_evi(nombre_archivo)

            # run ndvi
            anio, mes, dia = generar_nvdi(nombre_archivo)
            
            # insert evi to sql
            insert_evi_to_sql(anio, mes, dia)
            
            # insert ndvi to sql
            insert_ndvi_to_sql(anio, mes, dia)
            

    except requests.exceptions.HTTPError as e:

        # handle any errors here
        print(e)

def generar_evi(nombre_archivo):

    # constantes
    DATAFIELD_NAME  = "CMG 0.05 Deg 16 days EVI"
    LONG_MIN        = -118.2360
    LONG_MAX        = -86.1010
    LAT_MIN         = 12.3782
    LAT_MAX         = 33.5791

    # ciclo de procesamiento
    # init timer
    start_time = time.time()
    # generar nombres
    tipo, fecha, coleccion, produccion, extension = nombre_archivo.split(".")
    anio      = int(fecha[1:5])
    dia       = int(fecha[5:])
    new_fecha = datetime.date(anio,1,1) + datetime.timedelta(dia)
    new_fecha = new_fecha.strftime("%Y-%m-%d")
    anio, mes, dia = new_fecha.split("-")

    # procesamiento hdf
    hdf = SD("data/{}".format(nombre_archivo), SDC.READ)

    # leer el dataset
    data2D = hdf.select(DATAFIELD_NAME)
    data   = data2D[:,:].astype(np.double)

    # read attributes
    attrs       = data2D.attributes(full=1)
    lna         = attrs['long_name']
    long_name   = lna[0]
    vra         = attrs['valid_range']
    valid_range = vra[0]
    fva         = attrs['_FillValue']
    _FillValue  = fva[0]
    ua          = attrs['units']
    units       = ua[0]

    # Handle fill value
    invalid       = data == _FillValue
    invalid       = np.logical_or(invalid, data < valid_range[0])
    invalid       = np.logical_or(invalid, data > valid_range[1])
    data[invalid] = np.nan

    # apply scale factor and offset
    data = (data - 0.0) /  10000

    # normally we would use the grid metadata to reconstruct the grid, but
    # the grid metadata is incorrect
    x = np.linspace(-180, 180, 7200)
    y = np.linspace(90, -90, 3600)

    lon, lat = np.meshgrid(x,y)

    # init xi, yi, zi
    xi = []
    yi = []
    zi = []

    # ciclo
    for i in range(len(lon)):
        for j in range(len(lat)):
            xi.append(x[i])
            yi.append(y[j])
            zi.append(data[j,i])

    # generar arreglo de datos
    arr = np.stack((xi,yi,zi), axis=1)


    # columnas para el df
    cols = ['lon', 'lat', 'value']

    # crear data frame con la informacion del hdf
    df = pd.DataFrame(arr, columns=cols)

    # delimitar el area de estudio
    df = df.where((df['lon'] > LONG_MIN) & (df['lon'] < LONG_MAX)).dropna()
    df = df.where((df['lat'] > LAT_MIN) & (df['lat'] < LAT_MAX)).dropna()

    # obtener valores de x, y
    lons = np.array(df['lon'])
    lats = np.array(df['lat'])

    # agregar anio, mes y dia al data frame
    df['Anio'] = anio
    df['Mes']  = mes
    df['Dia']  = dia

    # titulo archivo
    titulo_archivo = "{}-{}-{}_EVI.csv".format(anio, mes, dia)

    # exportar df a csv
    df.to_csv("processing/{}".format(titulo_archivo), index=False)

    # print file
    print(titulo_archivo)

    # end time
    print("Tiempo de procesamiento: ", time.time() - start_time)
    

def generar_nvdi(nombre_archivo):
    # constantes
    DATAFIELD_NAME  = "CMG 0.05 Deg 16 days NDVI"
    LONG_MIN        = -118.2360
    LONG_MAX        = -86.1010
    LAT_MIN         = 12.3782
    LAT_MAX         = 33.5791

    # ciclo de procesamiento
    # init timer
    start_time = time.time()
    # generar nombres
    tipo, fecha, coleccion, produccion, extension = nombre_archivo.split(".")
    anio      = int(fecha[1:5])
    dia       = int(fecha[5:])
    new_fecha = datetime.date(anio,1,1) + datetime.timedelta(dia)
    new_fecha = new_fecha.strftime("%Y-%m-%d")
    anio, mes, dia = new_fecha.split("-")

    # procesamiento hdf
    hdf = SD("data/{}".format(nombre_archivo), SDC.READ)

    # leer el dataset
    data2D = hdf.select(DATAFIELD_NAME)
    data   = data2D[:,:].astype(np.double)

    # read attributes
    attrs       = data2D.attributes(full=1)
    lna         = attrs['long_name']
    long_name   = lna[0]
    vra         = attrs['valid_range']
    valid_range = vra[0]
    fva         = attrs['_FillValue']
    _FillValue  = fva[0]
    ua          = attrs['units']
    units       = ua[0]

    # Handle fill value
    invalid       = data == _FillValue
    invalid       = np.logical_or(invalid, data < valid_range[0])
    invalid       = np.logical_or(invalid, data > valid_range[1])
    data[invalid] = np.nan

    # apply scale factor and offset
    data = (data - 0.0) /  10000

    # normally we would use the grid metadata to reconstruct the grid, but
    # the grid metadata is incorrect
    x = np.linspace(-180, 180, 7200)
    y = np.linspace(90, -90, 3600)

    lon, lat = np.meshgrid(x,y)

    # init xi, yi, zi
    xi = []
    yi = []
    zi = []

    # ciclo
    for i in range(len(lon)):
        for j in range(len(lat)):
            xi.append(x[i])
            yi.append(y[j])
            zi.append(data[j,i])

    # generar arreglo de datos
    arr = np.stack((xi,yi,zi), axis=1)


    # columnas para el df
    cols = ['lon', 'lat', 'value']

    # crear data frame con la informacion del hdf
    df = pd.DataFrame(arr, columns=cols)

    # delimitar el area de estudio
    df = df.where((df['lon'] > LONG_MIN) & (df['lon'] < LONG_MAX)).dropna()
    df = df.where((df['lat'] > LAT_MIN) & (df['lat'] < LAT_MAX)).dropna()

    # obtener valores de x, y
    lons = np.array(df['lon'])
    lats = np.array(df['lat'])

    # agregar anio, mes y dia al data frame
    df['Anio'] = anio
    df['Mes']  = mes
    df['Dia']  = dia

    # titulo archivo
    titulo_archivo = "{}-{}-{}_NDVI.csv".format(anio, mes, dia)

    # exportar df a csv
    df.to_csv("processing/{}".format(titulo_archivo), index=False)

    # print file
    print(titulo_archivo)

    # end time
    print("Tiempo de procesamiento: ", time.time() - start_time)
    
    return anio, mes, dia

def insert_evi_to_sql(anio, mes, dia):
    # datos de la conexión
    conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.3.so.1.1};SERVER='+server+';DATABASE='+database+';UID='+usr_db+';PWD='+ pwd_db)
    cursor =  conn.cursor()
    
    nombre_del_archivo = "/home/jorge/Documents/Research/get_modis_files_from_nasa/processing/{}-{}-{}_EVI.csv".format(anio, mes, dia)
    
    df = pd.read_csv(nombre_del_archivo)
    
    df["temp"] = "{}-{}-{}".format(anio, mes, dia)
    
    df["fecha"] = pd.to_datetime(df["temp"])
    
    # delete temp column
    del df["temp"]

    for index, row in df.iterrows():
        """lon, lat, value, fecha"""
        
        LATS    = row["lat"]
        LONS    = row["lon"]
        VALUE   = row["value"]
        FECHA   = row["fecha"]
        

        # generar query
        query = "INSERT INTO EVI (lat, lon, valor, fecha) VALUES ((?),(?), (?), (?))"
        # ejecutar insert
        print(query)
        cursor.execute(query, (LATS, LONS, VALUE, FECHA))
        cursor.commit()
    conn.close()
    print("OK...")
    
def insert_ndvi_to_sql(anio, mes, dia):
    # datos de la conexión
    conn = pyodbc.connect('DRIVER={/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.3.so.1.1};SERVER='+server+';DATABASE='+database+';UID='+usr_db+';PWD='+ pwd_db)
    cursor =  conn.cursor()
    
    nombre_del_archivo = "/home/jorge/Documents/Research/get_modis_files_from_nasa/processing/{}-{}-{}_NDVI.csv".format(anio, mes, dia)
    
    df = pd.read_csv(nombre_del_archivo)
    
    df["temp"] = "{}-{}-{}".format(anio, mes, dia)
    
    df["fecha"] = pd.to_datetime(df["temp"])
    
    # delete temp column
    del df["temp"]

    for index, row in df.iterrows():
        """lon, lat, value, fecha"""
        
        LATS    = row["lat"]
        LONS    = row["lon"]
        VALUE   = row["value"]
        FECHA   = row["fecha"]
        

        # generar query
        query = "INSERT INTO NDVI (lat, lon, valor, fecha) VALUES ((?),(?), (?), (?))"
        # ejecutar insert
        print(query)
        cursor.execute(query, (LATS, LONS, VALUE, FECHA))
        cursor.commit()
    conn.close()
    print("OK...")


if __name__ == '__main__':
    main()
