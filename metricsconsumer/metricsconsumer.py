import json
import atexit
import configparser
import argparse

from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2

create_table_sql = 'CREATE TABLE IF NOT EXISTS website_metrics (event_time BIGINT PRIMARY KEY, metrics JSONB)'
select_count_table_sql = 'SELECT COUNT(*) FROM website_metrics'
insert_table_sql = 'INSERT INTO website_metrics VALUES (%s, %s) ON CONFLICT DO NOTHING'


def setup_db():
    db_config = config['db']

    uri = 'postgres://' + db_config['user'] + ':' + db_config['password'] + '@' + db_config['host'] + ':' + \
          db_config['port'] + '/' + db_config['db_name'] + '?sslmode=require'

    db_conn = psycopg2.connect(uri)
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute(create_table_sql)

    cursor.close()
    db_conn.commit()

    return db_conn


def get_total_metrics(db_conn):
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute(select_count_table_sql)
    r = cursor.fetchone()
    cursor.close()
    return r['count']


def consume_message(consumer, db_conn, cursor):
    raw_msgs = consumer.poll(timeout_ms=int(config['kafka']['consumer_poll_timeout']))
    for tp, messages in raw_msgs.items():
        for message in messages:
            print(message.value.decode('utf-8'))
            msg = json.loads(message.value.decode('utf-8'))
            cursor.execute(insert_table_sql, (str(msg['event_time']), message.value.decode('utf-8')))
    db_conn.commit()
    consumer.commit()


def run_consumer(consumer, db_conn):
    cursor = db_conn.cursor(cursor_factory=RealDictCursor)

    while True:
        consume_message(consumer, db_conn, cursor)


def exit_handler(consumer, db_conn):
    print('Closing db connection and kafka consumer before exiting the program')
    db_conn.commit()
    db_conn.close()
    consumer.close()


def get_consumer():
    kafka_config = config['kafka']

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_config['server'] + ':' + kafka_config['port'],
        auto_offset_reset=kafka_config['auto_offset_reset'],
        security_protocol=kafka_config['security_protocol'],
        group_id=kafka_config['group_id'],
        ssl_cafile=kafka_config['ssl_cafile'],
        ssl_certfile=kafka_config['ssl_certfile'],
        ssl_keyfile=kafka_config['ssl_keyfile'],
        consumer_timeout_ms=int(kafka_config['consumer_timeout_ms']),
    )

    consumer.subscribe([kafka_config['topic_name']])
    return consumer


def main():
    consumer = get_consumer()
    db_conn = setup_db()
    atexit.register(exit_handler, consumer, db_conn)

    run_consumer(consumer, db_conn)


def get_config_file_name():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", "-e", help="Environment in which to run the program.",
                        choices=['test', 'prod'],
                        required=True)
    args = parser.parse_args()

    return 'config/config_' + args.env + '.ini'


def init(config_file_name):
    config = configparser.ConfigParser()
    config.read(config_file_name)

    return config


if __name__ == "__main__":
    config = init(get_config_file_name())
    main()
