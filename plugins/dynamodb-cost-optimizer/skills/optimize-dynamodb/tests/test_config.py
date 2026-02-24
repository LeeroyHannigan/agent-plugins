"""Tests for config.py - credential handling, input parsing, validation."""
import json
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from config import get_client, parse_input, validate_keys, fail, STANDARD_TO_IA_RATIO, IA_TO_STANDARD_RATIO
from decimal import Decimal


class TestConstants(unittest.TestCase):

    def test_breakeven_ratios(self):
        self.assertAlmostEqual(float(STANDARD_TO_IA_RATIO), 0.4167, places=3)
        self.assertAlmostEqual(float(IA_TO_STANDARD_RATIO), 0.1333, places=3)

    def test_ratio_ordering(self):
        self.assertGreater(STANDARD_TO_IA_RATIO, IA_TO_STANDARD_RATIO)


class TestGetClient(unittest.TestCase):

    @patch('config.boto3')
    def test_returns_client(self, mock_boto):
        mock_boto.client.return_value = MagicMock()
        client = get_client('dynamodb', 'us-east-1')
        mock_boto.client.assert_called_once_with('dynamodb', region_name='us-east-1')
        self.assertIsNotNone(client)

    @patch('config.boto3')
    def test_no_credentials_exits(self, mock_boto):
        from botocore.exceptions import NoCredentialsError
        mock_boto.client.side_effect = NoCredentialsError()
        with self.assertRaises(SystemExit), patch('builtins.print'):
            get_client('dynamodb', 'us-east-1')

    @patch('config.boto3')
    def test_client_error_exits(self, mock_boto):
        from botocore.exceptions import ClientError
        mock_boto.client.side_effect = ClientError(
            {'Error': {'Code': 'InvalidRegion', 'Message': 'bad'}}, 'op')
        with self.assertRaises(SystemExit), patch('builtins.print'):
            get_client('dynamodb', 'us-east-1')


class TestParseInput(unittest.TestCase):

    def test_from_argv(self):
        with patch('sys.argv', ['script', '{"region":"us-east-1"}']):
            data = parse_input()
            self.assertEqual(data['region'], 'us-east-1')

    def test_from_stdin(self):
        with patch('sys.argv', ['script']), \
             patch('sys.stdin') as mock_stdin:
            mock_stdin.read.return_value = '{"tableName":"test"}'
            data = parse_input()
            self.assertEqual(data['tableName'], 'test')

    def test_invalid_json_exits(self):
        with patch('sys.argv', ['script', 'not json']):
            with self.assertRaises(SystemExit), patch('builtins.print'):
                parse_input()

    def test_empty_stdin_exits(self):
        with patch('sys.argv', ['script']), \
             patch('sys.stdin') as mock_stdin, \
             patch('builtins.print'):
            mock_stdin.read.return_value = ''
            with self.assertRaises(SystemExit):
                parse_input()


class TestValidateKeys(unittest.TestCase):

    def test_all_present(self):
        validate_keys({'region': 'us-east-1', 'prices': {}}, ['region', 'prices'])

    def test_missing_key_exits(self):
        with self.assertRaises(SystemExit), patch('builtins.print'):
            validate_keys({'region': 'us-east-1'}, ['region', 'prices'])

    def test_multiple_missing_exits(self):
        with self.assertRaises(SystemExit), patch('builtins.print'):
            validate_keys({}, ['region', 'prices', 'tableName'])


class TestFail(unittest.TestCase):

    def test_prints_json_error(self):
        with self.assertRaises(SystemExit) as ctx:
            with patch('builtins.print') as mock_print:
                fail('something broke')
        mock_print.assert_called_once()
        output = json.loads(mock_print.call_args[0][0])
        self.assertEqual(output['error'], 'something broke')

    def test_exits_with_code_1(self):
        with self.assertRaises(SystemExit) as ctx:
            with patch('builtins.print'):
                fail('err')
        self.assertEqual(ctx.exception.code, 1)


if __name__ == '__main__':
    unittest.main()
