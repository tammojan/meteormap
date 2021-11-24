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

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None

with open("rms-sites.json") as rmsfile:
    json_stations = json.load(rmsfile)

approx_location = {}  # map station id to approximate lon-lat

with open("exact_locations.pickle", "rb") as handle:
    exact_location = pickle.load(handle)

known_station_ids = []
for station in json_stations:
    station_ids = station["properties"]["id"].split(",")
    for station_id in station_ids:
        approx_location[station_id] = tuple(station["geometry"]["coordinates"])

with pysftp.Connection(
    "gmn.uwo.ca",
    username="tjdijkema",
    private_key="/Users/dijkema/.ssh/id_dsa",
    cnopts=cnopts,
) as sftp:
    all_station_ids = [
        station for station in sftp.listdir("files/extracted_data") if len(station) == 6
    ]

    new_station_ids = [
        station_id for station_id in all_station_ids if station_id not in exact_location
    ]

    for station_id in new_station_ids:
        stationpath = os.path.join("files/extracted_data", station_id)
        obslist = sorted(sftp.listdir(stationpath))
        configpath = os.path.join(stationpath, obslist[-1], ".config")
        with sftp.open(configpath) as configfile:
            config = configparser.ConfigParser(inline_comment_prefixes=(";",))
            config.read_file(configfile)
            longitude = config["System"]["Longitude"]
            latitude = config["System"]["Latitude"]
            exact_location[station_id] = (longitude, latitude)

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

with open("rms-sites.json", "w", encoding="utf8") as outfile:
    json.dump(json_stations, outfile, indent=4, ensure_ascii=False)
