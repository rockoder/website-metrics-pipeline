import json
import atexit

from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2

topic_name = 'website-metrics-test'
db_name = 'metricsdb_test'
uri = "postgres://avnadmin:t7lshsuw02az70km@pg-3c8e9bc0-ganesh-3896.aivencloud.com:18548/" \
      + db_name + "?sslmode=require"
create_table_sql = '''CREATE TABLE IF NOT EXISTS website_metrics (
   event_time bigint primary key,
   metrics jsonb
)'''
select_table_sql = '''SELECT * FROM  website_metrics'''


def setup_db_conn():
    db_conn = psycopg2.connect(uri)
    c = db_conn.cursor(cursor_factory=RealDictCursor)
    c.execute(create_table_sql)

    return c


def run_consumer(consumer, c):
    consumer.subscribe([topic_name])

    while True:
        raw_msgs = consumer.poll(timeout_ms=1000)

        for tp, messages in raw_msgs.items():
            for message in messages:
                print(message.value.decode('utf-8'))
                msg = json.loads(message.value.decode('utf-8'))
                insert_table_sql = 'INSERT INTO website_metrics VALUES (%s, %s)'
                c.execute(insert_table_sql, (str(msg['event_time']), message.value.decode('utf-8')))

        consumer.commit()


def exit_handler(consumer):
    print('Closing consumer before exiting the program')
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

    atexit.register(exit_handler, consumer)
    return consumer


def main():
    consumer = get_consumer()
    c = setup_db_conn()
    run_consumer(consumer, c)


if __name__ == "__main__":
    main()
