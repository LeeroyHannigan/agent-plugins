"""Tests for cw_batch, get_pricing, discover, and analyze_all orchestration."""
import json
import sys
import os
import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


class TestCwBatch(unittest.TestCase):

    @patch('cw_batch.get_client')
    def test_basic_query(self, mock_gc):
        from cw_batch import batch_get_metrics
        ts = datetime(2025, 1, 15, tzinfo=timezone.utc)
        mock_cw = MagicMock()
        mock_gc.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {
            'MetricDataResults': [
                {'Id': 'r0', 'Timestamps': [ts], 'Values': [42.0]},
            ],
        }

        result = batch_get_metrics('us-east-1', [
            {'id': 'r0', 'table': 'tbl', 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
        ], ts - timedelta(days=1), ts)

        self.assertEqual(len(result['r0']), 1)
        self.assertEqual(result['r0'][0]['value'], 42.0)

    @patch('cw_batch.get_client')
    def test_gsi_dimension_added(self, mock_gc):
        from cw_batch import batch_get_metrics
        ts = datetime(2025, 1, 15, tzinfo=timezone.utc)
        mock_cw = MagicMock()
        mock_gc.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {'MetricDataResults': []}

        batch_get_metrics('us-east-1', [
            {'id': 'g0', 'table': 'tbl', 'gsi': 'idx', 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
        ], ts - timedelta(days=1), ts)

        call_kwargs = mock_cw.get_metric_data.call_args[1]
        dims = call_kwargs['MetricDataQueries'][0]['MetricStat']['Metric']['Dimensions']
        self.assertEqual(len(dims), 2)
        self.assertEqual(dims[1]['Value'], 'idx')

    @patch('cw_batch.get_client')
    def test_pagination(self, mock_gc):
        from cw_batch import batch_get_metrics
        ts = datetime(2025, 1, 15, tzinfo=timezone.utc)
        mock_cw = MagicMock()
        mock_gc.return_value = mock_cw
        mock_cw.get_metric_data.side_effect = [
            {'MetricDataResults': [{'Id': 'r0', 'Timestamps': [ts], 'Values': [1.0]}], 'NextToken': 'tok'},
            {'MetricDataResults': [{'Id': 'r0', 'Timestamps': [ts + timedelta(minutes=5)], 'Values': [2.0]}]},
        ]

        result = batch_get_metrics('us-east-1', [
            {'id': 'r0', 'table': 'tbl', 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
        ], ts - timedelta(days=1), ts)

        self.assertEqual(len(result['r0']), 2)
        self.assertEqual(mock_cw.get_metric_data.call_count, 2)

    @patch('cw_batch.time.sleep')
    @patch('cw_batch.get_client')
    def test_retry_on_throttle(self, mock_gc, mock_sleep):
        from cw_batch import batch_get_metrics
        from botocore.exceptions import ClientError
        ts = datetime(2025, 1, 15, tzinfo=timezone.utc)
        mock_cw = MagicMock()
        mock_gc.return_value = mock_cw
        throttle_err = ClientError({'Error': {'Code': 'ThrottlingException', 'Message': 'Rate exceeded'}}, 'GetMetricData')
        mock_cw.get_metric_data.side_effect = [
            throttle_err,
            {'MetricDataResults': [{'Id': 'r0', 'Timestamps': [ts], 'Values': [1.0]}]},
        ]

        result = batch_get_metrics('us-east-1', [
            {'id': 'r0', 'table': 'tbl', 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
        ], ts - timedelta(days=1), ts)

        self.assertEqual(len(result['r0']), 1)
        mock_sleep.assert_called_once_with(1)

    @patch('cw_batch.time.sleep')
    @patch('cw_batch.get_client')
    def test_non_throttle_error_raises(self, mock_gc, mock_sleep):
        from cw_batch import batch_get_metrics
        from botocore.exceptions import ClientError
        ts = datetime(2025, 1, 15, tzinfo=timezone.utc)
        mock_cw = MagicMock()
        mock_gc.return_value = mock_cw
        mock_cw.get_metric_data.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'nope'}}, 'GetMetricData')

        with self.assertRaises(ClientError):
            batch_get_metrics('us-east-1', [
                {'id': 'r0', 'table': 'tbl', 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
            ], ts - timedelta(days=1), ts)

    @patch('cw_batch.get_client')
    def test_results_sorted_by_timestamp(self, mock_gc):
        from cw_batch import batch_get_metrics
        ts1 = datetime(2025, 1, 15, 10, 0, tzinfo=timezone.utc)
        ts2 = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
        mock_cw = MagicMock()
        mock_gc.return_value = mock_cw
        mock_cw.get_metric_data.return_value = {
            'MetricDataResults': [{'Id': 'r0', 'Timestamps': [ts1, ts2], 'Values': [2.0, 1.0]}],
        }

        result = batch_get_metrics('us-east-1', [
            {'id': 'r0', 'table': 'tbl', 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
        ], ts2, ts1)

        self.assertEqual(result['r0'][0]['value'], 1.0)
        self.assertEqual(result['r0'][1]['value'], 2.0)


class TestGetPricing(unittest.TestCase):

    @patch('get_pricing.get_client')
    def test_parses_pricing(self, mock_gc):
        from get_pricing import get_pricing
        mock_pricing = MagicMock()
        mock_gc.return_value = mock_pricing

        def make_price_item(group, usage, price, vol=''):
            return json.dumps({
                'product': {'attributes': {'group': group, 'usagetype': usage, 'volumeType': vol}},
                'terms': {'OnDemand': {'t1': {'priceDimensions': {'d1': {'pricePerUnit': {'USD': str(price)}}}}}},
            })

        mock_pricing.get_products.side_effect = [
            {'PriceList': [
                make_price_item('DDB-ReadUnits', '', 0.00000025),
                make_price_item('DDB-WriteUnits', '', 0.00000125),
            ]},
            {'PriceList': [
                make_price_item('', 'USE1-ReadCapacityUnit-Hrs', 0.00013),
                make_price_item('', 'USE1-WriteCapacityUnit-Hrs', 0.00065),
            ]},
            {'PriceList': [
                make_price_item('', '', 0.25, vol='Amazon DynamoDB'),
                make_price_item('', '', 0.10, vol='Amazon DynamoDB - IA'),
            ]},
        ]

        prices = get_pricing('us-east-1')
        self.assertEqual(prices['read_request'], 0.00000025)
        self.assertEqual(prices['write_request'], 0.00000125)
        self.assertEqual(prices['rcu_hour'], 0.00013)
        self.assertEqual(prices['standard_storage'], 0.25)
        self.assertEqual(prices['ia_storage'], 0.10)
        # Check aliases
        self.assertEqual(prices['standard_read'], prices['read_request'])
        self.assertEqual(prices['on_demand_write'], prices['write_request'])

    @patch('get_pricing.get_client')
    def test_pagination(self, mock_gc):
        from get_pricing import get_pricing
        mock_pricing = MagicMock()
        mock_gc.return_value = mock_pricing

        def make_item(group, price, vol=''):
            return json.dumps({
                'product': {'attributes': {'group': group, 'usagetype': '', 'volumeType': vol}},
                'terms': {'OnDemand': {'t1': {'priceDimensions': {'d1': {'pricePerUnit': {'USD': str(price)}}}}}},
            })

        mock_pricing.get_products.side_effect = [
            {'PriceList': [make_item('DDB-ReadUnits', 0.25e-6)], 'NextToken': 'page2'},
            {'PriceList': [make_item('DDB-WriteUnits', 1.25e-6)]},
            {'PriceList': [make_item('ReadCapacityUnit-Hrs', 0.00013), make_item('WriteCapacityUnit-Hrs', 0.00065)]},
            {'PriceList': [make_item('', 0.25, vol='Amazon DynamoDB')]},
        ]

        get_pricing('us-east-1')
        self.assertEqual(mock_pricing.get_products.call_count, 4)

    @patch('builtins.print')
    @patch('get_pricing.get_client')
    def test_missing_prices_fails_fast(self, mock_gc, mock_print):
        from get_pricing import get_pricing
        mock_pricing = MagicMock()
        mock_gc.return_value = mock_pricing
        mock_pricing.get_products.return_value = {'PriceList': []}

        with self.assertRaises(SystemExit):
            get_pricing('us-east-1')


class TestDiscover(unittest.TestCase):

    @patch('discover.get_client')
    def test_lists_all_tables(self, mock_gc):
        from discover import discover
        mock_ddb = MagicMock()
        mock_gc.return_value = mock_ddb
        mock_ddb.get_paginator.return_value.paginate.return_value = [
            {'TableNames': ['t1', 't2']},
        ]
        mock_ddb.describe_table.side_effect = [
            {'Table': {'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                       'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 5},
                       'ItemCount': 100, 'TableSizeBytes': 5000}},
            {'Table': {'BillingModeSummary': {'BillingMode': 'PAY_PER_REQUEST'},
                       'ProvisionedThroughput': {'ReadCapacityUnits': 0, 'WriteCapacityUnits': 0},
                       'ItemCount': 50, 'TableSizeBytes': 2000}},
        ]

        result = discover('us-east-1')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['billingMode'], 'PROVISIONED')
        self.assertEqual(result[1]['billingMode'], 'ON_DEMAND')

    @patch('discover.get_client')
    def test_specific_tables(self, mock_gc):
        from discover import discover
        mock_ddb = MagicMock()
        mock_gc.return_value = mock_ddb
        mock_ddb.describe_table.return_value = {
            'Table': {'BillingModeSummary': {'BillingMode': 'PROVISIONED'},
                      'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
                      'ItemCount': 10, 'TableSizeBytes': 100}}

        result = discover('us-east-1', ['my-table'])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['tableName'], 'my-table')
        mock_ddb.get_paginator.assert_not_called()

    @patch('discover.get_client')
    def test_error_on_single_table(self, mock_gc):
        from discover import discover
        mock_ddb = MagicMock()
        mock_gc.return_value = mock_ddb
        mock_ddb.describe_table.side_effect = Exception('not found')

        result = discover('us-east-1', ['bad-table'])
        self.assertEqual(len(result), 1)
        self.assertIn('error', result[0])


class TestAnalyzeAllOrchestration(unittest.TestCase):

    @patch('analyze_all.get_pricing')
    @patch('analyze_all.analyze_table')
    def test_single_region(self, mock_at, mock_gp):
        from analyze_all import analyze_all
        mock_gp.return_value = {'rcu_hour': 0.00013}
        mock_at.return_value = {
            'tableName': 't1', 'region': 'us-east-1', 'errors': [],
            'capacityMode': {'potentialMonthlySavings': 0},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }

        output = analyze_all({'region': 'us-east-1', 'tables': ['t1'], 'days': 14})
        self.assertIn('t1', output)
        self.assertIn('Already optimized', output)
        mock_at.assert_called_once()

    @patch('analyze_all.get_pricing')
    @patch('analyze_all.analyze_table')
    def test_multi_region(self, mock_at, mock_gp):
        from analyze_all import analyze_all
        mock_gp.return_value = {'rcu_hour': 0.00013}
        mock_at.side_effect = lambda r, t, d, p: {
            'tableName': t, 'region': r, 'errors': [],
            'capacityMode': {'potentialMonthlySavings': 0},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }

        output = analyze_all({'regions': {'us-east-1': ['t1'], 'eu-west-1': ['t2']}, 'days': 7})
        self.assertIn('t1', output)
        self.assertIn('t2', output)
        self.assertEqual(mock_gp.call_count, 2)

    @patch('analyze_all.get_pricing')
    @patch('analyze_all.analyze_table')
    def test_uses_provided_prices(self, mock_at, mock_gp):
        from analyze_all import analyze_all
        mock_at.return_value = {
            'tableName': 't1', 'region': 'us-east-1', 'errors': [],
            'capacityMode': {'potentialMonthlySavings': 0},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }

        analyze_all({'region': 'us-east-1', 'tables': ['t1'], 'days': 14, 'prices': {'rcu_hour': 0.1}})
        mock_gp.assert_not_called()


if __name__ == '__main__':
    unittest.main()
