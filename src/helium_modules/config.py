import os
import json
import helium_modules.helium_api as api
import helium_modules.kibana as kibana
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

#
# LOAD IN THE CONFIG FILE WHICH CONTAINS THE HOTSPOT ADDRESSES TO BE PROCESSED
#
def get_hotspots():
    configPath = "/data/config.json"
    configContents = open(configPath).read()
    configJson = json.loads(configContents)

    return configJson


def get_hotspot_details(hotspot_address):
    hotspot_config = kibana.get_document('helium-config', hotspot_address)

    if hotspot_config == None:
        hotspot_details = create_hotspot_config(hotspot_address)
    else:
        hotspot_details = extract_details_from_config(hotspot_config)

    return hotspot_details


#
# EXTRACT HOTSPOT DETAILS FROM CONFIG JSON
#
def extract_details_from_config(hotspot_config):
    config = hotspot_config['_source']

    hotspot_details = {
        'name': config['name'],
        'born_date': config['born_date'],
        'processed_date': config['processed_date'],
        'activity_count': config['activity_count']
    }

    return hotspot_details

#
# INSERT NEW HOTSPOT DETAILS INTO CONFIG
#
def create_hotspot_config(hotspot_address):
    hotspot_data = api.get_hotspot_data(hotspot_address)
    
    if hotspot_data['name'] == '':
        raise Exception('create_hotspot_config() error: ' + str(hotspot_data))

    activity_count = api.get_hotspot_activity_count(hotspot_address)

    hotspot_details = {
        'name': hotspot_data['name'],
        'born_date': hotspot_data['timestamp_added'],
        'processed_date': hotspot_data['timestamp_added'],
        'activity_count': activity_count
    }
    
    kibana.write_document('helium-config', hotspot_details, hotspot_address)

    return hotspot_details


#
# USED BY THE HISTORIC ACTIVITY PROCESSING
# WHEN ALL ACTIVITY HAS BEEN FETCHED BACK TO THE HOTSPOT BIRTH DATE
# THEN THE HISTORIC PROCESSING WILL NO LONGER BE PERFORMED FOR THAT 
#
def update_hotspot_earliest_processed_date(table, address, date):
    response = table.update_item(
        Key={
            'address': address
        },
        UpdateExpression="set earliest_processed_date=:lpd",
        ExpressionAttributeValues={
            ':lpd': date
        },
        ReturnValues="UPDATED_NEW"
    )

    if 'ResponseMetadata' in response:
        if 'HTTPStatusCode' in response['ResponseMetadata']:
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.debug("update_hotspot_earliest_processed_date() UPDATED: " + str(date))
                return True

    logger.error("update_hotspot_earliest_processed_date() FAILED: " + str(response))
    return False


