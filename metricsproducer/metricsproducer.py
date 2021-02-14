import re
import time
import json
import atexit

import requests
from kafka import KafkaProducer

# todo: take configs from command line
# todo: separate test and prod env configs
# todo: separate secretes
# todo: exception handling and flushing on error and also on exit
# todo: add logging

topic_name = 'website-metrics-test'
url = 'https://httpbin.org/get'
regex_pattern = 'origin'
pattern = re.compile(regex_pattern)
poll_interval = 5
time_precision = 1000
requests_timeout = 10


def create_message(event_time, response, regex_found):
    message = {
        'response_code': response.status_code,
        'response_time': response.elapsed.total_seconds(),
        'regex_found': regex_found,
        'regex_pattern': regex_pattern,
        'url': url,
        'event_time': event_time
    }

    message_json = json.dumps(message).encode("utf-8")

    print(message_json)
    return message_json


def process_website_metric(response):
    result = pattern.search(response.text)
    return False if result is None else True


def collect_website_metric():
    event_time = int(time.time() * time_precision)
    response = requests.get(url, timeout=requests_timeout)

    return event_time, response


def get_website_metric_message():
    event_time, response = collect_website_metric()
    regex_found = process_website_metric(response)
    message_json = create_message(event_time, response, regex_found)
    return message_json


def produce_metrics(producer):
    message_json = get_website_metric_message()
    producer.send(topic_name, message_json)


def run_producer(producer):
    while True:
        produce_metrics(producer)
        time.sleep(poll_interval)


def exit_handler(producer):
    print('Flushing producer before exiting the program')
    producer.flush()


def get_producer():
    producer = KafkaProducer(
        bootstrap_servers='kafka-31d994f-ganesh-3896.aivencloud.com:18550',
        security_protocol="SSL",
        ssl_cafile='secrets/ca.pem',
        ssl_certfile='secrets/service.cert',
        ssl_keyfile='secrets/service.key',
    )

    return producer


def main():
    producer = get_producer()
    atexit.register(exit_handler, producer)

    run_producer(producer)


if __name__ == "__main__":
    main()
