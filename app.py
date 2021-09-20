from api_connector.elastic_search import ElasticSearch
import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("-auth", "--Authentication", help="Authentication Parameter in header", required=True)
args = parser.parse_args()

if args.Authentication:
    with open('configuration/config.json', 'r') as file:
        config = json.load(file)
    config["configuration"]["authentication"] = args.Authentication
    with open('configuration/config.json', 'w') as file:
        json.dump(config, file)

dataStream = ElasticSearch()
dataStream.get_data_and_stream("events", "event-sso-*", "5h", 1000)



