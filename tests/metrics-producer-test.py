import unittest
import json
from unittest import mock
import responses
import metricsproducer


class TestMetricsProducer(unittest.TestCase):

    mock_time_now = 1613240662.386

    def setUp(self):

        self.mock_response_success = '{"args":{},"headers":{"Accept-Encoding":"gzip, deflate, br", ' \
                            '"origin":"1.2.3.4",' \
                            '"url":"https://httpbin.org/get"}'

        self.mock_event_message = '{"response_code": 200, "response_time": 0.491873, "regex_found": true, ' \
                         '"regex_pattern": "origin", "url": "https://httpbin.org/get", ' \
                         '"event_time": 1613240662386}'

    @responses.activate
    @mock.patch('time.time', mock.MagicMock(return_value=mock_time_now))
    def test_collect_website_metric(self):
        responses.add(
            responses.GET,
            metricsproducer.url,
            json=self.mock_response_success,
            status=200
        )
        message_json = metricsproducer.get_website_metric_message()

        msg_actual = json.loads(message_json)
        msg_expected = json.loads(self.mock_event_message)

        assert msg_actual['url'] == msg_expected['url']
        assert msg_actual['regex_found'] == msg_expected['regex_found']


if __name__ == '__main__':
    unittest.main()