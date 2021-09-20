import urllib3
import requests
import json
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# This disables all requests related Warnings.
urllib3.disable_warnings()

# logger config
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('data/app.log')
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# To get the Authentication parameter of the ElasticSearch request header which is user specific
# Change the config.json file accordingly
with open('configuration/config.json', 'r') as file:
    config = json.load(file)
    auth = config["configuration"]["authentication"]


def parse_time(time: str):
    unit = time[-1]
    delta = int(time[:-1])
    if unit == 's':
        return relativedelta(seconds=delta)
    elif unit == 'm':
        return relativedelta(minutes=delta)
    elif unit == 'h':
        return relativedelta(hours=delta)
    elif unit == 'D':
        return relativedelta(days=delta)
    elif unit == 'M':
        return relativedelta(months=delta)
    elif unit == 'Y':
        return relativedelta(years=delta)


# def send_kafka(kafka_topic: str, producer: KafkaProducer, data, page_no):
#     logger.debug(f"Page {page_no} sent through Kafka under topic {kafka_topic}")
#     for record in data:
#         producer.send(kafka_topic, record)
#     producer.flush()
#     logger.debug(f"Sent {len(data)} records")


class ElasticSearch:
    def __init__(self):
        # default
        self.lte = datetime.utcnow()
        self.gte = self.lte - timedelta(minutes=15)
        self.lte_str = self.lte.strftime("%Y-%m-%dT%H:%M:%S")
        self.gte_str = self.gte.strftime("%Y-%m-%dT%H:%M:%S")
        self.event = ""
        self.url = "https://scenvopswebstg.stg.secops.ibmcloudsecurity.com:5094/api/console/proxy?path=" + self.event + "%2F_search&method=POST"
        self.payload = {
              "size": 500,
              "query": {
                "bool": {
                  "must": [],
                  "filter": [
                    {
                      "match_all": {}
                    },
                    {
                      "range": {
                        "time": {
                          "format": "strict_date_optional_time",
                          "gte": self.gte_str,
                          "lte": self.lte_str
                        }
                      }
                    }
                  ],
                  "should": [],
                  "must_not": []
                }
              }
        }
        self.headers = {
            'Accept': 'application/json',
            'Authorization': auth,
            'Content-Type': 'application/json',
            'kbn-version': '7.4.2'
        }

        logger.debug("Initialized an ElasticSearch Object")

    def get_data(self, event_query: str, time: str = "15m", size: int = 500) -> list:
        """
        Get event data from Elastic Search API
        :param event_query: Provide the event query string  eg. event-sso-*
        :param size: The number of hits in one page
        :param time: The time filter for the results
        :return: Returns a list of pages of event json
        """
        logger.info("Get Data Operation Initiated")
        logger.debug(f"Getting data for {event_query} query of last {time}. Page size set to {size}.")
        if size > 10000:
            logger.debug(f"Page size >= 10000, not permitted. Setting page size to max value 10000.")
            size = 10000
        self.payload["size"] = size
        self.gte = self.lte - parse_time(time)
        self.gte_str = self.gte.strftime("%Y-%m-%dT%H:%M:%S")
        self.payload["query"]["bool"]["filter"][1]["range"]["time"]["gte"] = self.gte_str
        self.event = "%2F" + event_query
        self.url = self.url = "https://scenvopswebstg.stg.secops.ibmcloudsecurity.com:5094/api/console/proxy?path=" \
                              + self.event + "%2F_search%3Fscroll%3D1m&method=POST"

        scroll_req = "https://scenvopswebstg.stg.secops.ibmcloudsecurity.com:5094/api/console/proxy?path=_search%2Fscroll&method=POST"
        scroll_req_body = {
            "scroll": "1m",
            "scroll_id": ""
        }

        response_data = []

        # get first page
        response = requests.request("POST", self.url, headers=self.headers, data=json.dumps(self.payload), verify=False)
        if response.status_code == 200:
            content = json.loads(response.text)
            scroll_req_body["scroll_id"] = content["_scroll_id"]
            content = content["hits"]["hits"]
            response_data.append(content)
            logger.debug(f"Page {len(response_data)} received")
        else:
            logger.exception("Could not get response from server")
            raise Exception(f"Error {response.status_code}: Could not get response from server")

        # start scrolling
        logger.debug("Started Scrolling")
        while len(content) != 0:
            response = requests.request("POST", scroll_req, headers=self.headers, data=json.dumps(scroll_req_body), verify=False)
            if response.status_code == 200:
                content = json.loads(response.text)
                content = content["hits"]["hits"]
                if len(content) > 0:
                    response_data.append(content)
                    logger.debug(f"Page {len(response_data)} received")
            else:
                logger.exception("Could not get response from server")
                raise Exception(f"Error {response.status_code}: Could not get response from server")

        logger.info(f"Scrolled through {len(response_data)} pages")
        # delete scroll
        delete_scroll="https://scenvopswebstg.stg.secops.ibmcloudsecurity.com:5094/api/console/proxy?path=_search%2Fscroll&method=DELETE"
        delete_scroll_body = {
            "scroll_id": scroll_req_body["scroll_id"]
        }
        requests.request("DELETE", delete_scroll, headers=self.headers, data=json.dumps(delete_scroll_body), verify=False)
        logger.debug("Scroll id Deleted")

        return response_data

