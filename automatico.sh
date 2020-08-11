#!/bin/bash

# get today date
today="$(date '+%Y-%m-%d')"

echo $today

# change to work directory
cd /home/jorge/Documents/Research/get_modis_files_from_nasa/

# execute algoritmo.py
/home/jorge/anaconda3/bin/python3.7 /home/jorge/Documents/Research/get_modis_files_from_nasa/algoritmo.py
