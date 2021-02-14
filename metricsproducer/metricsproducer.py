import re
import time
import json
import atexit
import configparser
import argparse
import logging

import requests
from kafka import KafkaProducer

# todo: packaging/setup


def create_message(event_time, response, regex_found):
    website_config = config['website']

    message = {
        'response_code': response.status_code,
        'response_time': response.elapsed.total_seconds(),
        'regex_found': regex_found,
        'regex_pattern': website_config['regex_pattern'],
        'url': website_config['url'],
        'event_time': event_time
    }

    message_json = json.dumps(message).encode("utf-8")

    logging.debug(message_json)
    return message_json


def process_website_metric(response):
    result = pattern.search(response.text)
    return False if result is None else True


def collect_website_metric():
    event_time = int(time.time() * int(config['website']['time_precision']))

    for attempt in range(3):
        try:
            response = requests.get(config['website']['url'], timeout=int(config['website']['requests_timeout']))
            return event_time, response
        except Exception as err:
            logging.error('Attempt {} failed while connecting to the website:{}', attempt, err)
            time.sleep(1)


def get_website_metric_message():
    event_time, response = collect_website_metric()
    regex_found = process_website_metric(response)
    message_json = create_message(event_time, response, regex_found)
    return message_json


def produce_metrics(producer):
    message_json = get_website_metric_message()
    producer.send(config['kafka']['topic_name'], message_json)


def run_producer(producer):
    while True:
        produce_metrics(producer)
        time.sleep(int(config['website']['poll_interval']))


def exit_handler(producer):
    logging.info('Flushing producer before exiting the program')
    producer.flush()


def get_producer():
    kafka_config = config['kafka']

    producer = KafkaProducer(
        bootstrap_servers=kafka_config['server'] + ':' + kafka_config['port'],
        security_protocol=kafka_config['security_protocol'],
        ssl_cafile=kafka_config['ssl_cafile'],
        ssl_certfile=kafka_config['ssl_certfile'],
        ssl_keyfile=kafka_config['ssl_keyfile'],
    )

    return producer


def main():
    producer = get_producer()
    atexit.register(exit_handler, producer)

    run_producer(producer)


def get_config_file_name():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", "-e", help="Environment in which to run the program.",
                        choices=['test', 'prod'],
                        required=True)
    args = parser.parse_args()

    return 'config/config_' + args.env + '.ini'


def init(config_file_name):
    logfile = "logs/producer_{}.log".format(time.time())
    logging.basicConfig(filename=logfile,
                        format='%(asctime)s - %(thread)-20d - %(filename)-30s - %(lineno)-4d - '
                               '%(funcName)-40s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    config = configparser.ConfigParser()
    config.read(config_file_name)
    pattern = re.compile(config['website']['regex_pattern'])

    logging.info('Loaded config from:%s', config_file_name)
    return config, pattern


if __name__ == "__main__":
    config, pattern = init(get_config_file_name())
    main()
