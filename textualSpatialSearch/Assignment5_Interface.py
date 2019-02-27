#!/usr/bin/python2.7
#
# Assignment5 Interface
# Name: 
#

from pymongo import MongoClient
import os
import sys
import json
from math import sin, cos, atan2, radians, sqrt




def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    cityToSearch = cityToSearch.title()
    file = open(saveLocation1, "w")
    for documents in collection.find({"city":cityToSearch}):
        full_address = str(documents[u'full_address'].replace("\n", " ,"))
        output = documents[u'name'].upper() + "$" + full_address.upper() + "$" + documents[u'city'].upper() + "$" + documents[u'state'].upper() + "\n"
        file.write(output)

    file.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    file = open(saveLocation2, "w")
    lat1 = myLocation[0]
    lon1 = myLocation[1]
    obj_dict= {}
    for categories in categoriesToSearch:
        for documents in collection.find({u"categories":categories}):
            if DistanceFunction(documents[u'latitude'], documents[u'longitude'], float(lat1), float(lon1)) < maxDistance:
                if documents['_id'] not in obj_dict:
                    output = documents[u'name'].upper() + "\n"
                    file.write(output)
                    obj_dict[ documents['_id']] = documents[u'name']

    file.close()

def DistanceFunction(lat2,lon2,lat1,lon1):
    R = 3959
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    lon1 = radians(lon1)
    lon2 = radians(lon2)
    d1 = radians(lat2-lat1)
    d2 = radians(lon2-lon1)

    a = sin(d1/2)*sin(d1/2)+cos(lat1)*cos(lat2)*sin(d2/2)*sin(d2/2)
    c = 2*atan2(sqrt(a), sqrt(1-a))
    d = R*c
    return d