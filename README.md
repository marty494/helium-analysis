# helium-analysis

This is a Python application which fetches Helium Hotspot activity from the Helium API and persists it to ElasticSearch.
See the [Docker](#docker) section on how to launch this application.

## Pre-requisites:
It is necessary to have ElasticSearch running. It is necessary at the moment to have it running on the following endpoint*:
```
http://es01-dev:9200/
```
*This will be updated in a future release to be configurable


## Docker:
This application is available from DockerHub.
[helium-analysis](https://hub.docker.com/repository/docker/marty494/helium-analysis)

It is necessary to create a ```config.json``` file which contains the hotspot addresses you wish to process.
This is an example configuration file:
```
[
    { "hotspot_address": "31459265358979xxxBLAH99xxx7waBLAHbeeefffLAHPM3BLAHXX" },
    { "hotspot_address": "27812818281828xxxBLAH99xxx7waBLAHbeeefffLAHPM3BLAHXX" }
]
```

This is an example launch script for *nix (note the -v mounts a local volume which contains the configuration file):
```
LOG_LEVEL=$1

sudo docker run \
	--network elastic \
	-p 5000:5000 \
	-e LOGLEVEL="${LOG_LEVEL:-INFO}" \
	--name helium-analysis \
	-v /home/ubuntu/helium-analysis:/data \
	marty494/helium-analysis
```

## Helium API:
These are the Helium API endpoints used. See [Helium API reference](https://docs.helium.com/api/blockchain/introduction/)

### To obtain the creation date and name of the hotspot
This is performed the first time the hotspot is added and saved to ElasticSearch for future use:
```
/v1/hotspots/{hotspot_address}
```

### To fetch the data for a given time range
```
/v1/hotspots/{hotspot_address}/activity?filter_types=&min_time={min_time}&max_time={max_time}
```
This returns data and/or a cursor for the rest of the data paged by 100 records at a time.

### To fetch the next page of data
```
/v1/hotspots/{hotspot_address}/activity?cursor={cursor}
```

## Kibana / OpenSearch:
To visualise in Kibana the following script field(s) are useful:

To see the HNT earned you will need to convert the "rewards.amount" from Bones (100 million per HNT) to HNT.
Each record may contain zero, one, or more rewards and therefore you will need to sum up the values:

- Name: HNT
- Type: Number
- Format: 0,0.[00000000]
Script:
```
 for (config in children) {
                  if (!config.containsKey('stock')) {
                    childrenAreMatching.add(false);
                    continue;
                  } else if (!config['stock']['is_in_stock']
                      || config['special_price'] == null
                      || config['special_from_date'] == null 
                      || config['special_to_date'] == null) {
                    childrenAreMatching.add(false);
                    continue;
                  }
```
