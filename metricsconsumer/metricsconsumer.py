import json
import atexit

from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2

topic_name = 'website-metrics-test'
db_name = 'metricsdb_test'
uri = 'postgres://avnadmin:t7lshsuw02az70km@pg-3c8e9bc0-ganesh-3896.aivencloud.com:18548/' + \
      db_name + '?sslmode=require'
create_table_sql = 'CREATE TABLE IF NOT EXISTS website_metrics (event_time BIGINT PRIMARY KEY, metrics JSONB)'
select_count_table_sql = 'SELECT COUNT(*) FROM website_metrics'
insert_table_sql = 'INSERT INTO website_metrics VALUES (%s, %s) ON CONFLICT DO NOTHING'
consumer_poll_timeout = 10000


def setup_db():
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
    raw_msgs = consumer.poll(timeout_ms=consumer_poll_timeout)
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
    consumer = KafkaConsumer(
        bootstrap_servers='kafka-31d994f-ganesh-3896.aivencloud.com:18550',
        auto_offset_reset='earliest',
        security_protocol="SSL",
        group_id='metrics-consumer-group',
        ssl_cafile='secrets/ca.pem',
        ssl_certfile='secrets/service.cert',
        ssl_keyfile='secrets/service.key',
        consumer_timeout_ms=1000,
    )

    consumer.subscribe([topic_name])
    return consumer


def main():
    consumer = get_consumer()
    db_conn = setup_db()
    atexit.register(exit_handler, consumer, db_conn)

    run_consumer(consumer, db_conn)


if __name__ == "__main__":
    main()
