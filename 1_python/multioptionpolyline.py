# coding: utf-8

# In[ ]:


import matplotlib.cm as cm
import matplotlib as mpl
import folium
import csv
from branca.element import Figure

from jinja2 import Template
from six import text_type, binary_type

from branca.utilities import (
    _parse_size, _locations_mirror, _locations_tolist, image_to_url,
    none_min, none_max, iter_points
)
from branca.element import (Element, Figure, JavascriptLink, CssLink,
                            MacroElement)
from .map import Layer, Icon, Marker, Popup, FeatureGroup


width, height = '100%', '100%'

fig = Figure(width=width, height=height)


class multiOptionsPolyline(MacroElement):

    def __init__(self, locations, altitude, weight=None,
                 opacity=None, latlon=True, popup=None):
        super(multiOptionsPolyline, self).__init__()
        self._name = 'multiOptionsPolyline'
        self.data = (_locations_mirror(locations) if not latlon else
                     _locations_tolist(locations))
        self.altitude = altitude
        self.weight = weight
        self.opacity = opacity
        if isinstance(popup, text_type) or isinstance(popup, binary_type):
            self.add_child(Popup(popup))
        elif popup is not None:
            self.add_child(popup)

        self._template = Template("""u
            {% macro script(this, kwargs) %}

                var {{this.get_name()}} = L.multiOptionsPolyline(
                    {{this.data}}, {
                    multiOptions: {
                        optionIdxFn: function ({{this.altitude}}) {
                            var i, alt = {{this.altitude}},
                            altThresholds = [800, 900, 1000, 1100, 1200, 1300, 1400, 1500];

                            for (i = 0; i < altThresholds.length; ++i) {
                                if (latLng.alt <= altThresholds[i]) {
                                return i;
                                }
                            }
                        return altThresholds.length;
                        },
                        options: [
                            {color: '#0000FF'}, {color: '#0040FF'}, {color: '#0080FF'},
                            {color: '#00FFB0'}, {color: '#00E000'}, {color: '#80FF00'},
                            {color: '#FFFF00'}, {color: '#FFC000'}, {color: '#FF0000'}
                        ]
                    },
                    weight: 5,
                    lineCap: 'butt',
                    opacity: 0.75,
                    smoothFactor: 1}).addLayer({{this.get_name()}});
            {% endmacro %}
            """)  # noqa

    def _get_self_bounds(self):
        """
        Computes the bounds of the object itself (not including it's children)
        in the form [[lat_min, lon_min], [lat_max, lon_max]]
        """
        bounds = [[None, None], [None, None]]
        for point in iter_points(self.data):
            bounds = [
                [
                    none_min(bounds[0][0], point[0]),
                    none_min(bounds[0][1], point[1]),
                ],
                [
                    none_max(bounds[1][0], point[0]),
                    none_max(bounds[1][1], point[1]),
                ],
            ]
        return bounds


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
            my_helipads.append([data[0], data[1]])
    return my_helipads


def get_travel_data():
    all_flights = []
    with open('') as csvfile:
        datareader = csv.reader(csvfile, delimiter= ' ', quotechar='|')
        this_icao = None
        points_per_icao = []
        for row in datareader:
            data = row[0].split(',')
            if this_icao is not data[0]:
                this_icao = data[0]
                all_flights.append([this_icao, points_per_icao])
                points_per_icao = []
            else:
                points_per_icao.append([data[1], data[2], data[3]])

MAP_INIT = (55, 10)

# object_data = pd.read_csv("")

MAX_RECORDS = 100

helipad_locs = get_pad_data()
all_icao, all_x_y, all_x, all_y, all_alt = get_travel_data()

ave_lat = sum(p[0] for p in all_x_y)/len(all_x_y)
ave_lon = sum(p[1] for p in all_x_y)/len(all_x_y)

map = folium.Map(location=[ave_lat, ave_lon], zoom_start=4)

count = 1
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

folium.multiOptionsPolyline(all_x_y, all_alt).add_to(map)

map