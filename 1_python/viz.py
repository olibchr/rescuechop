# coding: utf-8

# In[ ]:


import numpy as np
import matplotlib
import pandas as pd
import hide_code.hide_code as hc
import folium
import xml.etree.ElementTree as ET
from math import cos, sin, sqrt, atan2
import math
import csv

def get_data():
    my_helipads = []
    with open('pads_at_hosps.csv', 'rb') as csvfile:
        datareader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in datareader:
            my_helipads.append(row)


MAP_INIT = (55, 10)

# object_data = pd.read_csv("")

MAX_RECORDS = 100


map = folium.Map(location=MAP_INIT, zoom_start=4)

all_helipads_x, all_helipads_y = get_Helipads()
all_hosp_x, all_hosp_y, all_hosp_name = get_Hospitals()

# pd_pads = pd.DataFrame({all_helipads_x, all_helipads_y})
# pd_hosps = pd.DataFrame({all_hosp_x, all_hosp_y, all_hosp_name})

pads_at_hosp = []
for i in range(1,len(all_helipads_x)):

    for j in range(1,len(all_hosp_x)):
        dist = haversine(all_helipads_x[i], all_helipads_y[i], all_hosp_x[j], all_hosp_y[j])
        if dist < 0.5:
            pads_at_hosp.append([all_helipads_x[i], all_helipads_y[i]])

with open("pads_at_hosps.csv"):
    writer = csv.writer(delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    for item in pads_at_hosp:
        writer.writerow([item[0], item[1]])

"""
for pad in pd_pads[1:MAX_RECORDS]:
    folium.Marker(
        location = [pad[0],pad[1]],
        icon=folium.Icon(color='blue'),
        popup=pad[2]).add_to(map)



for hosp in pd_hosps[1:MAX_RECORDS]:
    folium.Marker(
        location = [hosp[0],hosp[1]],
        icon=folium.Icon(color='red'),
        popup=pad[2]).add_to(map)

map
"""