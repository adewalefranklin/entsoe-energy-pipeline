import os, json

import urllib.parse, urllib.request

from datetime import datetime, timedelta, timezone

from xml.etree import ElementTree as ET

import boto3



s3 = boto3.client("s3")



BUCKET = "day-ahead-prices-adewale-franklin"

BASE\_URL = "https://web-api.tp.entsoe.eu/api"





ZONES = {

    "DE-LU": "10Y1001A1001A82H",

    "FR":    "10YFR-RTE------C",

    "NL":    "10YNL----------L",

    "BE":    "10YBE----------2",

    "AT":    "10YAT-APG------L",

    "IT1":   "10Y1001A1001A73I",

    "ES":    "10YES-REE------0",

    "PT":    "10YPT-REN------W",

    "PL":    "10YPL-AREA-----S",

    "SE4":   "10Y1001A1001A47J"

}





\# 1. Download XML from ENTSO-E



def get\_xml(params):

    url = BASE\_URL + "?" + urllib.parse.urlencode(params)

    return urllib.request.urlopen(url).read().decode("utf-8")







\# 2. Extract only Points



def extract\_points(xml\_text):

    root = ET.fromstring(xml\_text)

    ns = root.tag.split("}")\[0] + "}" if root.tag.startswith("{") else ""



    points = \[]



    for p in root.findall(f".//{ns}Point"):

        pos = int(p.find(f"{ns}position").text)

        price = float(p.find(f"{ns}price.amount").text)



        points.append({"position": pos, "price": price})



    return points

