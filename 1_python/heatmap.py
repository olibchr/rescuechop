# coding: utf-8

# In[ ]:


import matplotlib.cm as cm
import matplotlib as mpl
import folium
import csv
from branca.element import Figure
from folium import plugins


width, height = '100%', '100%'

fig = Figure(width=width, height=height)


def get_color(altitude):
    norm = mpl.colors.Normalize(vmin=0, vmax=3000)
    cmap = cm.hot
    m = cm.ScalarMappable(norm=norm, cmap=cmap)
    return m.to_rgba(altitude)


def get_pad_data():
    my_helipads = []
    with open('../2_data/pads_at_hosps.csv', 'rb') as csvfile:
        datareader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in datareader:
            data = row[0].split(',')
            my_helipads.append(tuple([data[0], data[1]]))
    return my_helipads


def get_travel_data():
    all_icao = []
    all_x = []
    all_y = []
    all_x_y = []
    all_alt = []
    with open('../2_data/aircraft_positions_pretty_time.csv') as csvfile:
        datareader = csv.reader(csvfile, delimiter=',', quotechar='|')
        # this_icao = 'foo'
        # points_per_icao = []
        for row in datareader:
            #data = row[0].split(',')
            data = row
            all_icao.append(data[0])
            all_x.append(float(data[2]))
            all_y.append(float(data[3]))
            all_x_y.append([float(data[2]), float(data[3])])
            if data[4] is not None:
                all_alt.append((data[4]))
            """
            if str(this_icao) != str(data[0]):
                this_icao = data[0]
                points_per_icao.append([data[1], data[2], data[3]])
                all_flights.append([this_icao, points_per_icao])
                points_per_icao = []
            else:
                this_icao = data[0]
                points_per_icao.append([data[1], data[2], data[3]])"""

    return all_icao, all_x_y, all_x, all_y, all_alt


MAP_INIT = (55, 10)

# object_data = pd.read_csv("")

MAX_RECORDS = 100

helipad_locs = get_pad_data()
all_icao, all_x_y, all_x, all_y, all_alt = get_travel_data()

ave_lat = sum(p[0] for p in all_x_y)/len(all_x_y)
ave_lon = sum(p[1] for p in all_x_y)/len(all_x_y)

map = folium.Map(location=[ave_lat, ave_lon], zoom_start=4)


hm = plugins.HeatMap(all_x_y)
map.add_children(hm)

map
#fig.add_child(map)
#fig