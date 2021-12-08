#!/usr/bin/env python

import pickle
import pprint
import json
from geopy.distance import distance

pp = pprint.PrettyPrinter(indent=2)

with open("exact_locations.pickle", "rb") as handle:
    exact_location = pickle.load(handle)

with open("rms-sites.json") as rmsfile:
    json_stations = json.load(rmsfile)

def json_stationid(i):
    return json_stations[i]["properties"]["id"]

approx_location = {}

stationid_in_json = {}
for i, station in enumerate(json_stations):
    station_ids = station["properties"]["id"].split(",")
    for station_id in station_ids:
        stationid_in_json[station_id] = i
        approx_location[station_id] = tuple(station["geometry"]["coordinates"])

locations_in_json = {}
for stationid in stationid_in_json:
    location = exact_location[stationid]
    if location not in locations_in_json:
        locations_in_json[location] = set([stationid_in_json[stationid]])
    else:
        locations_in_json[location].add(stationid_in_json[stationid])

duplicates = [locations_in_json[l] for l in locations_in_json if len(locations_in_json[l])>1]

# Find stations where approx location is far from exact location
for stationid in approx_location:
    dist = distance(reversed(approx_location[stationid]), reversed(list(map(float, exact_location[stationid])))).km
    if dist > 3.5:
        print(stationid, approx_location[stationid], exact_location[stationid], dist, sep='\t')

# Remove one duplicate (run many times to fix all)
for dup in duplicates:
    print('---')
    dupl = sorted(list(dup))
    d0 = dupl[0]
    for d in dup:
       print(dup, json_stationid(d))
    extra = ","+",".join([json_stationid(d) for d in dupl[1:]])
    json_stations[d0]["properties"]["id"] += extra
    for d in dupl[-1:0:-1]: # Backward otherwise indices will mismatch
        del json_stations[d]
    break

with open("rms-sites.json", "w", encoding="utf8") as outfile:
    json.dump(json_stations, outfile, indent=4, ensure_ascii=False)
    outfile.write("\n")
