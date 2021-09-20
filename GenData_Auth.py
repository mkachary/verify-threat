"""
_source.data.result
            .subtype
            .origin
            .cause
            .username

        .year
        .month
        .time
        .day
"""

import time
import json
import string
import random
import ndjson
import os
from datetime import datetime, timedelta
# from pyspark.sql import SparkSession, Row


# with open("data/auth.json") as read_file:
#     record = json.load(read_file)


def random_alphanumeric(length: int):
    s = length
    ran = ''.join(random.choices(string.ascii_lowercase + string.digits, k=s))
    return str(ran)


def generate_id():
    s1 = random_alphanumeric(8)
    s2 = random_alphanumeric(4)
    s3 = random_alphanumeric(4)
    s4 = random_alphanumeric(4)
    s5 = random_alphanumeric(12)
    return s1 + "-" + s2 + "-" + s3 + "-" + s4 + "-" + s5


def gen_data(total: int, users: int, ip: int):
    """
    Generate Random Authentication Events
    :param total: Total numbers of events you want to generate
    :param users: Number of Users you want
    :param ip: Number of ips you want + 1 (For 3 ips: ip = 4)
    :return: Return a list of authentication events
    """
    records = []
    now = datetime.now()
    timediff = timedelta(milliseconds=0.0)

    start = time.time()

    for i in range(total):
        r_user = random.randint(1, users)
        r_ip = random.randint(2, ip)
        result = "failure" if (random.randint(0, 1000) % 2 == 0) else "success"

        event = {
            "_index": "event-authentication-2021.4-000001",
            "_type": "_doc",
            "_id": "acf65bed-c553-4ec0-a49a-86b9ed03fe7b",
            "_version": 1,
            "_score": None,
            "_source": {
                "geoip": {
                    "continent_name": "Asia",
                    "city_name": None,
                    "country_iso_code": "IN",
                    "country_name": "India",
                    "region_name": None,
                    "location": {
                        "lon": "77.0",
                        "lat": "20.0"
                    }
                },
                "data": {
                    "result": "success",
                    "subtype": "user_password",
                    "subject": "6150002AYI",
                    "origin": "49.36.33.104",
                    "cause": "Authentication Successful",
                    "action": "login",
                    "sourcetype": "clouddirectory",
                    "realm": "cloudIdentityRealm",
                    "devicetype": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36",
                    "target": "https://siddharaj-rel.rel.verify.ibmcloudsecurity.com/oidc/endpoint/default/authorize?qsId=0493f5d7-e222-4973-8811-2a20c967139c&client_id=usc-client",
                    "username": "Siddharaj"
                },
                "year": 2021,
                "event_type": "authentication",
                "month": 4,
                "indexed_at": 1618469336329,
                "@processing_time": 43,
                "tenantid": "3a68acd0-2d60-4b99-bd2b-a2dadf126387",
                "tenantname": "siddharaj-rel.rel.verify.ibmcloudsecurity.com",
                "correlationid": "CORR_ID-573abe27-d399-4930-a4cd-bedb00d238b6",
                "servicename": "authsvc",
                "id": "acf65bed-c553-4ec0-a49a-86b9ed03fe7b",
                "time": 1618469336286,
                "day": 15
            }
        }

        if result == "failure":
            event["_source"]["data"]["result"] = "failure"
            event["_source"]["data"][
                "cause"] = "CSIAH0303E Incorrect user ID or password. Enter a valid user ID or password." \
                           " Contact your administrator for more information."
            event["_source"]["data"]["subject"] = "UNKNOWN"

        event["_id"] = generate_id()
        event["_source"]["data"]["id"] = event["_id"]
        event["_source"]["data"]["username"] = "User_" + str(r_user)
        event["_source"]["data"]["origin"] = "9.199.234." + str(r_ip)

        event["_source"]["year"] = now.year
        event["_source"]["month"] = now.month
        event["_source"]["day"] = now.day
        event["_source"]["indexed_at"] = int(now.timestamp() * 1000)
        event["_source"]["time"] = int(now.timestamp() * 1000)

        now += timediff
        records.append(event.copy())

    end = time.time()
    print(f"Took {end - start}seconds")
    return records


if __name__=="__main__":
    filepath = 'data/fake1.ndjson'
    if os.path.isfile(filepath):
        os.remove(filepath)
    else:
        print('The output file doesnt exist')
    data = gen_data(50000, 1200, 200)
    with open(filepath, 'a+') as f:
        for i in data:
            json.dump(i, f)
            f.write("\n")
