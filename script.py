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
from pyhdf.SD import SD, SDC
from access import usr, pwd

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

    except requests.exceptions.HTTPError as e:

        # handle any errors here
        print(e)


if __name__ == '__main__':
    main()
