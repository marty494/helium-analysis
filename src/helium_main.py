import os
import pytz
from datetime import datetime, timedelta
from dateutil import parser
import helium_modules.helium_api as api
import helium_modules.kibana as kibana
import helium_modules.config as config
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

#
# THE MAIN PROCESSING LOOP - PROCESSES EACH CONFIGURED HOTSPOT
#
def process_hotspots(run_date):
    logger.info('process_hotspots() run_date: ' + str(run_date))
    hotspots = config.get_hotspots()

    for hotspot in hotspots:
        process_hotspot(hotspot['hotspot_address'], run_date)

#
# PROCESSES THE SUPPLIED HOTSPOT UNTIL THE RUN DATE IS REACHED
# RETRIEVES AND POPULATES HOTSPOT DETAILS IF NECESSARY
#
def process_hotspot(hotspot_address, run_date):
    logger.debug('process_hotspot() hotspot_address: ' + hotspot_address)

    try:
        more_data = True
        while more_data:
            hotspot_details = config.get_hotspot_details(hotspot_address)
            logger.debug('process_hotspot() hotspot_details: ' + str(hotspot_details))
            more_data = process_activity(hotspot_address, hotspot_details, run_date)

    except Exception as error:
        logger.exception('process_hotspot() error: ' + str(error))
        return


#
# FETCH NEW ACTIVITY FOR HOTSPOT AND SPECIFIED DATE RANGE
# RETURNS THE TRUE IF ALL ACTIVITY HAS BEEN PROCESSED
# FALSE MEANS THERE IS STILL MORE ACTIVITY AVAILABLE FOR LATER DATES
#
def process_activity(hotspot_address, hotspot_details, run_date):
    logger.info('process_activity() name: ' + hotspot_details['name'])

    processed_date = parser.parse(hotspot_details['processed_date'])
    born_date = parser.parse(hotspot_details['born_date'])

    min_date = processed_date.replace(hour=0, minute=0, second=0)
    min_date = min_date - timedelta(days=1)
    if min_date < born_date:
        min_date = born_date
    max_date = processed_date.replace(hour=0, minute=0, second=0)
    max_date = max_date + timedelta(days=1)
    if max_date > run_date:
        max_date = run_date

    logger.info('process_activity() min_date: ' + str(min_date) + ', max_date: ' + str(max_date))

    response = api.get_hotspot_activity(hotspot_address, min_date, max_date)

    index = hotspot_details['name']

    if 'data' in response:
        if len(response['data']) > 0:
            persist_data(hotspot_address, index, response['data'])

    while 'cursor' in response:
        response = api.get_hotspot_activity_cursor(hotspot_address, response['cursor'])
        if 'data' in response:
            if len(response['data']) > 0:
                persist_data(hotspot_address, index, response['data'])

    hotspot_details['processed_date'] = max_date.astimezone(pytz.utc).isoformat()
    config.update_hotspot_config(hotspot_address, hotspot_details)

    return max_date < run_date


#
# PREPARE AND PERSIST THE ACTIVITY
#
def persist_data(hotspot_address, index, data):
    logger.debug('persist_data() index: ' + index)

    for document in data:
        logger.debug('==================================================')
        logger.debug('persist_data() document: ' + str(document))
        if 'hash' in document and 'time' in document:
            if kibana.document_exists(index, document['hash']):
                continue

            document = transform_time(document)
            kibana.write_document(index, document, document['hash'])

            # Do not update the config time at this point.
            # The order of processing is not chronological and if this process
            # does not complete successfully, it may not process some earlier
            # entries. Also, updating the config everytime is not very efficient


#
# DETERMINES IF ANY ACTIVITY HAS OCCURRED SINCE THE LAST CHECK
# COMPARES THE ACTIVITY PREVIOUSLY RETRIEVED AGAINST THE NEW ACTIVITY COUNTS
#
def is_hotspot_activity(hotspot_address, hotspot_details):
    activity_count = api.get_hotspot_activity_count(hotspot_address)
    logger.info('OLD Activity count: ' + str(hotspot_details['activity_count']))
    logger.info('NEW Activity count: ' + str(activity_count))
    if str(activity_count) != hotspot_details['activity_count']:
        return True
    else:
        return False


#
# FOR THE PURPOSING OF TEMPORAL INDEXING IT IS NECESSARY TO TURN THE TIME
# FIELD INTO A DATE RATHER THAN MILLISECONDS. WE COULD HAVE ADDED AN ADDITIONAL
# DATE FIELD, BUT THE TIME IN MS IS NOT OF ANY USE
#
def transform_time(document):
    time_ms = document['time']
    str_date = datetime.fromtimestamp(time_ms)
    str_date = str_date.astimezone(pytz.utc).isoformat()
    str_date = str_date.replace("+00:00", "Z").replace(" ", "T")
    document['time'] = str_date
    return document

#
# THE ENTRY POINT WHEN LAUNCHING
#
if __name__ == '__main__':
    run_date = datetime.now(pytz.utc)
    logger.info('Loading Helium function at time: ' + run_date.astimezone().isoformat())
    api.set_domain_endpoint()
    process_hotspots(run_date)

