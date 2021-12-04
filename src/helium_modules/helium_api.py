import urllib3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DOMAIN_ENDPOINT = 'https://api.helium.io/v1/hotspots/'
# DOMAIN_ENDPOINT = 'https://helium-api.stakejoy.com/v1/hotspots'

#
# OBTAINS THE COUNTERS FOR THE SUPPLIED HOTSPOT
# THIS IS USED TO IDENTIFY WHEN ACTIVITY HAS OCCURRED TO MINIMIZE API CALLS
#
def get_hotspot_activity_count(hotspot_address):
    logger.debug('get_hotspot_activity_count for address: ' + hotspot_address)
    
    api = DOMAIN_ENDPOINT + hotspot_address + '/activity/count'
    http = urllib3.PoolManager()
    response = http.request('GET', api)
    response = json.loads(response.data.decode('utf-8'))
    
    return response

#
# CURRENTLY ONLY USED TO OBTAIN THE BIRTH DATE FOR THE HOTSPOT
# ONCE THE BIRTH DATE HAS BEEN ADDED TO THE CONFIG DATABASE THEN THIS 
# ENDPOINT WILL NO LONGER BE USED FOR THIS HOTSPOT
#
def get_hotspot_data(hotspot_address):
    logger.debug('get_hotspot_data for address: ' + hotspot_address)
    
    api = DOMAIN_ENDPOINT + hotspot_address
    http = urllib3.PoolManager()
    response = http.request('GET', api)
    response = json.loads(response.data.decode('utf-8'))

    if 'data' in response:
        hotspot_data = {
            'name': response['data']['name'],
            'timestamp_added': response['data']['timestamp_added']
        }
    else:
        hotspot_data = {
            'name': '',
            'timestamp_added': ''
        }
    
    return hotspot_data

#
# FETCH ALL ACTIVITY FOR THE SPECIFIED HOTSPOT BETWEEN THE SUPPLIED DATES
# THIS IS USED FIRSTLY FOR THE INITIAL HISTORIC ACTIVITY LOADING AND
# THEN FOR THE CONTINUOUS PERIODIC ACTIVITY LOADING
#
def get_hotspot_activity(hotspot_address, min_time, max_time):
    logger.debug('get_hotspot_activity - address: ' + hotspot_address + ', min_time: ' + str(min_time) + ', max_time: ' + str(max_time))

    api = DOMAIN_ENDPOINT + hotspot_address + "/activity?filter_types=&min_time=" + min_time + '&max_time=' + max_time
    http = urllib3.PoolManager()
    response = http.request('GET', api)
    response = json.loads(response.data.decode('utf-8'))

    return response

#
# THIS IS USED TO FETCH THE NEXT DATA FOR THE SPECIFIED CURSOR
# IT WILL INITIALLY FOLLOW AN ACTIVITY ENDPOINT INVOCATION WHICH WOULD SPECIFY 
# THE SEARCH CRITERIA. IT MAY ALSO FOLLOW A PREVIOUS CALL TO THIS ENDPOINT
#
def get_hotspot_activity_cursor(hotspot_address, cursor):
    logger.debug('get_hotspot_activity_cursor - address: ' + hotspot_address)

    api = DOMAIN_ENDPOINT + hotspot_address + '/activity?cursor=' + cursor
    http = urllib3.PoolManager()
    response = http.request('GET', api)
    response = json.loads(response.data.decode('utf-8'))

    return response    

