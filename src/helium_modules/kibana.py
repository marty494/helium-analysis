import os
import json
import requests
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

headers = { "Content-Type": "application/json" }
host = 'http://es01-dev:9200/'

#
# LOOKUP A DOCUMENT AND RETURN TRUE IF IT IS FOUND
#
def document_exists(index, document_id):
    url = host + index + '/_doc/' + document_id
    response = requests.head(url)
   
    logger.debug('document_exists() response: ' + str(response))
    logger.debug('document_exists() index: ' + index + ', document_id: ' + document_id)

    return True


#
# WRITE DOCUMENT TO ELASTICSEARCH
# RETURN A CONFLICT STATUS CODE IF THE ENTRY ALREADY EXISTS
#
def write_document(index, document, document_id):
    uri = host + index + '/_create/' + document_id
    r = requests.put(uri, json=document, headers=headers)

    if r.status_code == requests.codes.created:
        logger.debug('write_document() CREATED document: ' + str(document))
    else:
        if r.status_code == requests.codes.conflict:
            if document['error']['type'] == 'version_conflict_engine_exception':
                # Already exists, just skip
                logger.debug('write_document() ALREADY EXISTS document: ' + str(document))
                return True
            else:
                raise Exception(r.text)
        else:
            raise Exception(r.text)


#
# RETRIEVE DOCUMENT FROM ELASTICSEARCH
#
def get_document(index, hotspot_address):
    uri = host + index + '/_doc/' + hotspot_address
    r = requests.get(uri, headers=headers)

    logger.debug('r.status_code: ' + str(r.status_code))
    logger.debug('r.text: ' + r.text)

    document = r.json()

    if r.status_code == requests.codes.ok:
        document = r.json() #json.loads(r.text)
    else:
        if r.status_code == requests.codes.not_found:
            if 'error' in document:
                if document['error']['type'] == 'index_not_found_exception':
                    return None
            else:
                if document['found'] == False:
                    return None
                else:
                    raise Exception(r.text)
        else:
            raise Exception(r.text)

    return document