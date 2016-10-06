import xml.etree.ElementTree as ET
import csv
from math import radians, cos, sin, asin, sqrt


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def get_Hospitals():

    all_hosp_x = []
    all_hosp_y = []
    all_hosp_name = []
    tree = ET.parse('../2_data/europe_hospitals.osm.xml')
    root = tree.getroot()
    for child in root:
        lon = child.get('lon')
        lat = child.get('lat')

        name = ''
        for tag in child:
            if tag.get('k') is 'name':
                name = tag.get('v')
                break


        all_hosp_x.append(lat)
        all_hosp_y.append(lon)
        all_hosp_name.append(name)
    return all_hosp_x, all_hosp_y, all_hosp_name


def get_Helipads():

    all_helipads_x = []
    all_helipads_y = []
    tree = ET.parse('../2_data/europe_helipads.osm.xml')
    root = tree.getroot()
    for child in root:
        lon = child.get('lon')
        lat = child.get('lat')

        all_helipads_x.append(lat)
        all_helipads_y.append(lon)
    return all_helipads_x, all_helipads_y


all_helipads_x, all_helipads_y = get_Helipads()
all_hosp_x, all_hosp_y, all_hosp_name = get_Hospitals()

# pd_pads = pd.DataFrame({all_helipads_x, all_helipads_y})
# pd_hosps = pd.DataFrame({all_hosp_x, all_hosp_y, all_hosp_name})

pads_at_hosp = []
for i in range(1, len(all_hosp_x)):
    for j in range(1, len(all_helipads_x)):
        dist = haversine(float(all_hosp_x[i]), float(all_hosp_x[i]), float(all_helipads_x[j]), float(all_helipads_x[j]))
        if dist < 1:
            pads_at_hosp.append([all_helipads_x[j], all_helipads_y[j]])
            break

with open("pads_at_hosps.csv", 'wb') as csvfile:
    writer = csv.writer(csvfile, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
    for item in pads_at_hosp:
        writer.writerow([item[0], item[1]])