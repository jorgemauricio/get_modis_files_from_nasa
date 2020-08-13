#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
#######################################################
# Generar archivo CSV de un HDF5 para la variable NDVI
# Author: Jorge Mauricio
# Email: jorge.ernesto.mauricio@gmail.com
# Date: Created on Thu Sep 28 08:38:15 2017
# Version: 1.0
#######################################################
"""

# libreria
import time
import datetime
import os
import re
import pandas as pd
import numpy as np
from pyhdf.SD import SD, SDC

# funcion main
def main():
    # constantes
    DATAFIELD_NAME  = "CMG 0.05 Deg 16 days NDVI"
    LONG_MIN        = -118.2360
    LONG_MAX        = -86.1010
    LAT_MIN         = 12.3782
    LAT_MAX         = 33.5791

    # lista de archivos a procesar
    lista_de_archivos = [x for x in os.listdir("data") if x.endswith(".hdf")]

    # ciclo de procesamiento
    for archivo in lista_de_archivos:
        # generar nombres
        tipo, fecha, coleccion, produccion, extension = archivo.split(".")
        anio      = int(fecha[1:5])
        dia       = int(fecha[5:])
        new_fecha = datetime.date(anio,1,1) + datetime.timedelta(dia)
        new_fecha = new_fecha.strftime("%Y-%m-%d")
        anio, mes, dia = new_fecha.split("-")

        # procesamiento hdf
        hdf = SD("data/{}".format(archivo), SDC.READ)

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

    # frames
    frames = []

    # lista de archivos procesados
    lista = [x for x in os.listdir("processing") if x.endswith("NDVI.csv")]

    # generar un solo archivo
    resultado = pd.concat(frames)

    # guardar a csv
    resultado.to_csv("results/compilado_NDVI.csv")


if __name__ == '__main__':
    main()
