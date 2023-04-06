#!/usr/bin/env python

"""Read config files from RMS sftp server, randomize locations, write to json file for meteor map"""

import pysftp
import json
import os
import configparser
import sys
from tqdm import tqdm
import pickle
import numpy as np
import requests
from bs4 import BeautifulSoup

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with open("rms-sites.json") as rmsfile:
    json_stations = json.load(rmsfile)

approx_location = {}  # map station id to approximate lon-lat

try:
    with open("exact_locations.pickle", "rb") as handle:
        exact_location = pickle.load(handle)
    print(f"{len(exact_location)} stations in exact")
except FileNotFoundError:
    exact_location = {}

known_station_ids = []
for station in json_stations:
    station_ids = station["properties"]["id"].split(",")
    for station_id in station_ids:
        approx_location[station_id] = tuple(station["geometry"]["coordinates"])

ignored_stations = ["RU000B", "RU000C", "RU000K", "RU000L", "NZ000E"]
def removeInlineComments(cfgparser, delimiter):
    """ Removes inline comments from config file. """
    for section in cfgparser.sections():
        [cfgparser.set(section, item[0], item[1].split(delimiter)[0].strip()) for item in cfgparser.items(section)]

with pysftp.Connection(
    "gmn.uwo.ca",
    username="tjdijkema",
    private_key="/Users/dijkema/.ssh/id_rsa_rms",
    cnopts=cnopts,
) as sftp:
    all_station_ids = [
        station for station in sftp.listdir("files/extracted_data") if len(station) == 6
    ]

    if len(all_station_ids) == 0:
        raise RuntimeError("No data found at all, something's wrong with the server")

    print(len(all_station_ids), "stations on the server")

    new_station_ids = [
        station_id for station_id in all_station_ids if station_id not in exact_location
    ]

    for station_id in new_station_ids:
        if station_id in ignored_stations:
            continue
        try:
            stationpath = os.path.join("files/extracted_data", station_id)
            obslist = sorted(sftp.listdir(stationpath))
            configpath = os.path.join(stationpath, obslist[-1], ".config")
            with sftp.open(configpath) as configfile:
                config = configparser.ConfigParser(inline_comment_prefixes=(";",))
                config.read_file(configfile)
                removeInlineComments(config, ';')
                longitude = config["System"]["Longitude"]
                latitude = config["System"]["Latitude"]
                exact_location[station_id] = (longitude, latitude)
        except Exception as e:
            print("Error with station", station_id)
            print(str(e))
            continue

with open("exact_locations.pickle", "wb") as handle:
    pickle.dump(exact_location, handle, protocol=pickle.HIGHEST_PROTOCOL)

new_station_ids = [
    station_id for station_id in all_station_ids if station_id not in approx_location
]
exact_location_inv = {}
for station_id in exact_location:
    if station_id not in new_station_ids:
        exact_location_inv[exact_location[station_id]] = station_id


def random_offset(lon, lat):
    """Apply random offset to longitude and latitude (in degrees)"""
    u = np.random.random()
    v = np.random.random()
    r = 2000 / 111300.0  # 111300 meters per degree, 2000m radius
    w = r * np.sqrt(u)
    t = 2 * np.pi * v
    new_lon = float(lon) + w * np.cos(t) / np.cos(np.deg2rad(float(lat)))
    new_lat = float(lat) + w * np.sin(t)
    return round(new_lon, 3), round(new_lat, 3)


for station_id in new_station_ids:
  if station_id in ignored_stations:
    continue
  try:
    if not station_id in approx_location:
        if exact_location[station_id] in exact_location_inv:
            print(
                f"station id {station_id} matches {exact_location_inv[exact_location[station_id]]}"
            )
        else:
            new_lon, new_lat = random_offset(*exact_location[station_id])
            new_station = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [new_lon, new_lat]},
                "properties": {"name": "", "id": station_id},  # TODO: fetch from istra
            }
            json_stations.append(new_station)
            exact_location_inv[exact_location[station_id]] = station_id
  except Exception as e:
      print(f"Problem with station {station_id}:")
      print(e)
      continue
# Update names from istrastream
url = 'http://istrastream.com/rms-gmn/'
soup = BeautifulSoup(requests.get(url).content, 'html.parser')
rows = soup.select("tr")
stationnames = {}
stationlens = {}
for row in rows[:-1]:
    texts = [child.text for child in row.children]
    station_id = texts[5]
    station_name = texts[7]
    station_lens = texts[9]
    if station_id in exact_location:
        stationnames[station_id] = station_name
        stationlens[station_id] = station_lens

for station in json_stations:
    stationids = station["properties"]["id"].split(",")
    has_match = False
    for stationid in stationids:
        if stationid in stationnames:
            has_match = True
            break
    if not has_match:
        continue
    station["properties"]["name"] = stationnames[stationid]
    station["properties"]["lens"] = stationlens[stationid]
    station["properties"]["link"] = "http://istrastream.com/rms-gmn/?id=" + stationid

with open("rms-sites.json", "w", encoding="utf8") as outfile:
    json.dump(json_stations, outfile, indent=4, ensure_ascii=False)
    outfile.write("\n")
