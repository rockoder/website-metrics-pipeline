import unittest
import json
from unittest import mock
import responses
import metricsproducer


class TestMetricsProducer(unittest.TestCase):

    mock_time_now = 1613240662.386

    def setUp(self):
        self.mock_response_success = {
            'args': {},
            'headers': {
                'Accept-Encoding': 'gzip, deflate, br',
                'origin': '1.2.3.4',
                'url': 'https://httpbin.org/get'
            }
        }

        self.mock_metric_message = {
            'response_code': 200,
            'response_time': 0.491873,
            'regex_found': True,
            'regex_pattern': 'origin',
            'url': 'https://httpbin.org/get',
            'event_time': self.mock_time_now * 1000
        }

    @responses.activate
    @mock.patch('time.time', mock.MagicMock(return_value=mock_time_now))
    def test_collect_metric_pattern_success(self):
        responses.add(responses.GET, metricsproducer.url, json=json.dumps(self.mock_response_success), status=200)
        metric_message_json = metricsproducer.get_website_metric_message()

        metric_message = json.loads(metric_message_json)

        assert metric_message['url'] == self.mock_metric_message['url']
        assert metric_message['regex_found'] == self.mock_metric_message['regex_found']


if __name__ == '__main__':
    unittest.main()