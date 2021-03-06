import unittest
from psycopg2.extras import RealDictCursor
import metricsproducer
import metricsconsumer


class TestMetricsProducer(unittest.TestCase):

    def setUp(self):
        metricsproducer.config, metricsproducer.pattern = metricsproducer.init('config/config_test.ini')
        metricsconsumer.config = metricsconsumer.init('config/config_test.ini')

        self.producer = metricsproducer.get_producer()
        self.consumer = metricsconsumer.get_consumer()
        self.db_conn = metricsconsumer.setup_db()
        self.cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)

    def test_website_metric_end_to_end(self):
        metricsproducer.produce_metrics(self.producer)

        before_count = metricsconsumer.get_total_metrics(self.db_conn)
        metricsconsumer.consume_message(self.consumer, self.db_conn, self.cursor)
        after_count = metricsconsumer.get_total_metrics(self.db_conn)
        self.db_conn.close()

        assert after_count == before_count + 1


if __name__ == '__main__':
    unittest.main()
