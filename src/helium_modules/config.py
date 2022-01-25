import os
import json
import pytz
from datetime import datetime
from dateutil import parser
import helium_modules.helium_api as api
import helium_modules.elastic as elastic
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

#
# LOAD IN THE CONFIG FILE WHICH CONTAINS THE HOTSPOT ADDRESSES TO BE PROCESSED
# AND THE ANTENNA DATA WHICH IS ADDED ON TO THE ELASTIC DATA
#
# THE antennas ARRAY MUST HAVE THE FOLLOWING FIELDS:
# - id: ANY INTEGER IN ANY ORDER (FOR DATE SORTING PURPOSES)
# - date: YYYY-MM-DD IN ANY ORDER (WILL BE SORTED BY APPLICATION)
# - mast_ft: ANY INTEGER (HEIGHT IN FEET OF THE MAST)
# - details: DESCRIPTION OF ANTENNA CONFIGURATION
#
# REQUIRED FORMAT EXAMPLE:
# [
#   {
#     "hotspot_address": "1111111111aaaaaaaaaaBBBBBBBBBB9999999999zzzzzzzzzzZ",
#     "antennas": [
#         { "id": 1, "date":"2021-09-26", "dbi": 4.5, "mast_ft: 0, "details": "Bedroom + 1m cable + stock antenna" },
#         { "id": 2, "date":"2021-09-27", "dbi": 8.5, "mast_ft: 0, "details": "Loft + 1m cable + Paradar antenna" },
#         { "id": 3, "date":"2021-12-19", "dbi": 6.0, "mast_ft: 11, "details": "Chimney + 5m LMR-400 cable + pigtail + Paradar antenna" }
#     ]
#   },
#   {
#     "hotspot_address": "2222222222bbbbbbbbbbCCCCCCCCCC5555555555xxxxxxxxxxY",
#   }
# ]
#
def get_hotspots():
    configPath = "/data/config.json"
    configContents = open(configPath).read()
    configJson = json.loads(configContents)

    return configJson

# GET THE ANTENNA DATA FROM THE CONFIG FOR THE SPECIFIED HOTSPOT
# THE DATA IS THEN SORTED WITH THE LATEST DATE FIRST
# IF NOT FOUND THEN RETURN AN EMPTY STRING
def get_antennas(hotspot):
    sorted_antennas = ''
    if 'antennas' in hotspot:
        antennas = hotspot['antennas']
        sorted_antennas = sorted(antennas, 
            key=lambda antenna: datetime.strptime(antenna['date'], '%Y-%m-%d'), reverse=True)
    return sorted_antennas

#
# GET THE SPECIFIED HOTSPOT CONFIG DETAILS
# IF NOT FOUND THEN CREATE THE CONFIG FOR THIS HOTSPOT
#
def get_hotspot_details(hotspot_address):
    hotspot_config = elastic.get_document('helium-config', hotspot_address)

    if hotspot_config == None:
        hotspot_details = create_hotspot_config(hotspot_address)
    else:
        hotspot_details = extract_hotspot_details_from_config(hotspot_config)

    return hotspot_details

#
# GET THE SPECIFIED COIN CONFIG DETAILS
# IF NOT FOUND THEN CREATE THE CONFIG FOR THIS COIN
#
def get_coin_details(coin):
    coin_config = elastic.get_document('coin-config', coin)

    if coin_config == None:
        coin_details = create_coin_config(coin)
    else:
        coin_details = extract_coin_details_from_config(coin_config)

    return coin_details

#
# EXTRACT HOTSPOT DETAILS FROM CONFIG JSON
#
def extract_hotspot_details_from_config(hotspot_config):
    config = hotspot_config['_source']

    hotspot_details = {
        'name': config['name'],
        'born_date': config['born_date'],
        'processed_date': config['processed_date'],
        'activity_count': config['activity_count']
    }

    return hotspot_details

#
# EXTRACT COIN DETAILS FROM CONFIG JSON
#
def extract_coin_details_from_config(coin_config):
    config = coin_config['_source']

    coin_details = {
        'earliest_coin_date': config['earliest_coin_date'],
        'latest_coin_date': config['latest_coin_date']
    }

    return coin_details

#
# INSERT NEW HOTSPOT DETAILS INTO CONFIG
#
def create_hotspot_config(hotspot_address):
    hotspot_data = api.get_hotspot_data(hotspot_address)
    
    if hotspot_data['name'] == '':
        raise Exception('create_hotspot_config() error: ' + str(hotspot_data))

    activity_count = api.get_hotspot_activity_count(hotspot_address)

    timestamp_added = parser.parse(hotspot_data['timestamp_added'])

    hotspot_details = {
        'name': hotspot_data['name'],
        'born_date': timestamp_added.astimezone(pytz.utc).isoformat(),
        'processed_date': timestamp_added.astimezone(pytz.utc).isoformat(),
        'activity_count': activity_count
    }
    
    elastic.write_document('helium-config', hotspot_details, hotspot_address)

    return hotspot_details


#
# INSERT DEFAULT COIN DETAILS INTO CONFIG
#
def create_coin_config(coin):
    now = datetime.now(pytz.utc)

    coin_details = {
        'earliest_coin_date': now.astimezone(pytz.utc).isoformat(),
        'latest_coin_date': now.astimezone(pytz.utc).isoformat()
    }

    elastic.write_document('coin-config', coin_details, coin)

    return coin_details


#
# UPDATED EXISTING HOTSPOT DETAILS
#
def update_hotspot_config(hotspot_address, hotspot_details):
    elastic.update_document('helium-config', hotspot_details, hotspot_address)

#
# UPDATED EXISTING COIN DETAILS
#
def update_coin_config(coin, coin_details):
    elastic.update_document('coin-config', coin_details, coin)
