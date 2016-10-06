# coding: utf-8

# In[ ]:


import matplotlib.cm as cm
import matplotlib as mpl
import matplotlib.colors as colors
import folium
import csv
from branca.element import Figure


width, height = '100%', '100%'

fig = Figure(width=width, height=height)


def get_color(altitude):
    norm = mpl.colors.Normalize(vmin=0, vmax=3000)
    cmap = cm.nipy_spectral
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
    with open('../2_data/aircraft_positions_sample2.csv') as csvfile:
        datareader = csv.reader(csvfile, delimiter= ' ', quotechar='|')
        # this_icao = 'foo'
        # points_per_icao = []
        for row in datareader:
            data = row[0].split(',')
            all_icao.append((data[0]))
            all_x.append(float(data[1]))
            all_y.append(float(data[2]))
            all_x_y.append([float(data[1]), float(data[2])])
            all_alt.append(float(data[3]))
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

count = 0

"""
for pad in helipad_locs[1:]:
    folium.Marker(
        location = [pad[0],pad[1]],
        icon=folium.Icon(color='blue')
        ).add_to(map)
    count += 1"""
"""
for flight in flights[:MAX_RECORDS]:
    folium.Marker(
        location = [flight[0],flight[1]],
        icon=folium.Icon(color='red')
        ).add_to(map)
"""
last_plane = all_x_y[0]
for plane in all_x_y[1:]:
    this_plane = [last_plane] + [plane]
    last_plane = plane

    this_color = get_color(all_alt[count])
    this_color = colors.rgb2hex(this_color)
    count += 1
    folium.PolyLine(this_plane, color=this_color, weight=2.5, opacity=1).add_to(map)

map
#fig.add_child(map)
#fig