import pandas as pd
import folium
import gpxpy
import os

def overlayGPX(gpxData, map):
    gpx_file = open(gpxData, 'r')
    gpx = gpxpy.parse(gpx_file)
    points = []
    for track in gpx.tracks:
        for segment in track.segments:
            for point in segment.points:
                points.append(tuple([point.latitude, point.longitude]))
    latitude = sum(p[0] for p in points)/len(points)
    longitude = sum(p[1] for p in points)/len(points)
    folium.PolyLine(points, color="red", weight=2.5, opacity=1).add_to(map)

df = pd.read_csv('data_giri.csv', index_col=0)

m = folium.Map(location=[46.3833318, 11.8499966], zoom_start = 9)
folium.TileLayer('openstreetmap').add_to(m)
folium.TileLayer('Stamen Terrain').add_to(m)
folium.LayerControl().add_to(m)

for subdir, dirs, files in os.walk(os.getcwd() + "\\gpx\\"):
    for file in files:
        filepath = subdir + os.sep + file
        if filepath.endswith(".gpx"):
            overlayGPX(filepath, m)

for i, txt in enumerate(df['Name']):
    coords = [float(df["Latitude"][i]), float(df["Longitude"][i])]
    text = "<b>" + txt + "</b>\n" + str(df["Data"][i])
    tipo = df["Tipologia"][i]
    if df["Tipologia"][i] == "Cima":
        folium.Marker(coords, icon=folium.Icon(color="green", icon="mountain", prefix='fa'), popup=text, tooltip=txt).add_to(m)
    elif df["Tipologia"][i] == "Anello":
        folium.Marker(coords, icon=folium.Icon(color="blue", icon="ring", prefix='fa'), popup=text, tooltip=txt).add_to(m)
    else:
        folium.Marker(coords, icon=folium.Icon(color="red", icon="house", prefix='fa'), popup=text, tooltip=txt).add_to(m)

m.save("map.html")