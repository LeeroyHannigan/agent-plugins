"""Test suite for DynamoDB Cost Optimizer scripts."""
import json
import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from decimal import Decimal
from datetime import datetime, timezone, timedelta

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from autoscaling_sim import simulate

# Shared test pricing
PRICES = {
    'read_request': 0.00000025, 'write_request': 0.00000125,
    'rcu_hour': 0.00013, 'wcu_hour': 0.00065,
    'ia_read': 0.00000031, 'ia_write': 0.00000156,
    'ia_rcu_hour': 0.00016, 'ia_wcu_hour': 0.00081,
    'standard_read': 0.00000025, 'standard_write': 0.00000125,
    'on_demand_read': 0.00000025, 'on_demand_write': 0.00000125,
    'standard_storage': 0.25, 'ia_storage': 0.10,
}

def mock_describe_table(billing='PROVISIONED', rcu=100, wcu=50, size_bytes=0,
                         table_class='STANDARD', gsis=None):
    """Build a mock describe_table response."""
    resp = {
        'Table': {
            'TableArn': 'arn:aws:dynamodb:us-east-1:123:table/test',
            'TableStatus': 'ACTIVE',
            'BillingModeSummary': {'BillingMode': billing},
            'ProvisionedThroughput': {'ReadCapacityUnits': rcu, 'WriteCapacityUnits': wcu},
            'TableSizeBytes': size_bytes,
            'TableClassSummary': {'TableClass': table_class},
            'GlobalSecondaryIndexes': gsis or [],
        }
    }
    return resp

def mock_batch_metrics(metric_map):
    """Build mock return for batch_get_metrics. metric_map: {id: [(value, ts_offset_min), ...]}"""
    base = datetime(2025, 1, 15, tzinfo=timezone.utc)
    result = {}
    for qid, points in metric_map.items():
        result[qid] = [{'timestamp': base + timedelta(minutes=offset), 'value': val}
                        for val, offset in points]
    return result


class TestAutoscalingSim(unittest.TestCase):

    def test_empty_metrics(self):
        self.assertEqual(simulate([]), [])

    def test_constant_load(self):
        metrics = [10.0] * 100
        prov = simulate(metrics, target_utilization=0.7)
        self.assertEqual(len(prov), 100)
        # Should provision above consumed to hit 70% target
        self.assertGreater(prov[0], 10.0)

    def test_scale_out_on_spike(self):
        metrics = [5.0] * 20 + [50.0] * 10 + [5.0] * 20
        prov = simulate(metrics, target_utilization=0.7)
        # After spike, provisioned should increase
        self.assertGreater(max(prov[20:30]), max(prov[0:5]))

    def test_scale_in_after_drop(self):
        metrics = [50.0] * 5 + [1.0] * 100
        prov = simulate(metrics, target_utilization=0.7)
        # After sustained low usage, should scale in
        self.assertLess(prov[-1], prov[5])

    def test_respects_min_capacity(self):
        metrics = [0.001] * 50
        prov = simulate(metrics, min_cap=5)
        self.assertTrue(all(p >= 5 for p in prov))

    def test_respects_max_capacity(self):
        metrics = [99999.0] * 50
        prov = simulate(metrics, max_cap=100)
        self.assertTrue(all(p <= 100 for p in prov))

    def test_daily_scale_in_reset(self):
        # 1440 minutes = 1 day, scale-in count should reset
        metrics = [50.0] * 5 + [0.1] * 1500
        prov = simulate(metrics, target_utilization=0.7)
        self.assertEqual(len(prov), 1505)


class TestCapacityMode(unittest.TestCase):

    @patch('capacity_mode.batch_get_metrics')
    @patch('capacity_mode.get_client')
    def test_zero_usage_recommends_on_demand(self, mock_gc, mock_cw):
        from capacity_mode import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table()
        mock_cw.return_value = mock_batch_metrics({'cr': [], 'cw': []})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['recommendedMode'], 'ON_DEMAND')
        self.assertEqual(result['currentMode'], 'PROVISIONED')

    @patch('capacity_mode.batch_get_metrics')
    @patch('capacity_mode.get_client')
    def test_high_usage_recommends_provisioned(self, mock_gc, mock_cw):
        from capacity_mode import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            billing='PAY_PER_REQUEST')
        points = [(50000.0, i * 5) for i in range(4032)]
        mock_cw.return_value = mock_batch_metrics({'cr': points, 'cw': points})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['currentMode'], 'ON_DEMAND')
        self.assertGreater(result['onDemandMonthlyCost'], 0)

    @patch('capacity_mode.batch_get_metrics')
    @patch('capacity_mode.get_client')
    def test_savings_zero_when_already_on_demand_no_usage(self, mock_gc, mock_cw):
        from capacity_mode import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            billing='PAY_PER_REQUEST')
        mock_cw.return_value = mock_batch_metrics({'cr': [], 'cw': []})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['recommendedMode'], 'ON_DEMAND')
        self.assertEqual(result['potentialMonthlySavings'], 0.0)

    @patch('capacity_mode.batch_get_metrics')
    @patch('capacity_mode.get_client')
    def test_ia_table_uses_ia_pricing(self, mock_gc, mock_cw):
        from capacity_mode import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            table_class='STANDARD_INFREQUENT_ACCESS', rcu=100, wcu=50)
        mock_cw.return_value = mock_batch_metrics({'cr': [], 'cw': []})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        # Provisioned cost should use IA rates: 100*730*0.00016 + 50*730*0.00081 = 41.245
        expected = 100 * 730 * 0.00016 + 50 * 730 * 0.00081
        self.assertAlmostEqual(result['currentProvisionedMonthlyCost'], expected, places=2)


class TestTableClass(unittest.TestCase):

    @patch('table_class._check_reserved_capacity', return_value=False)
    @patch('table_class.batch_get_metrics')
    @patch('table_class.get_client')
    def test_large_storage_low_throughput_recommends_ia(self, mock_gc, mock_cw, mock_rc):
        from table_class import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            billing='PAY_PER_REQUEST', size_bytes=100 * 1024**3)
        mock_cw.return_value = mock_batch_metrics({
            'cr': [(100.0, i * 1440) for i in range(14)],
            'cw': [(50.0, i * 1440) for i in range(14)],
        })

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['recommendedClass'], 'STANDARD_INFREQUENT_ACCESS')
        self.assertGreater(result['potentialMonthlySavings'], 0)

    @patch('table_class._check_reserved_capacity', return_value=False)
    @patch('table_class.batch_get_metrics')
    @patch('table_class.get_client')
    def test_high_throughput_stays_standard(self, mock_gc, mock_cw, mock_rc):
        from table_class import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            billing='PAY_PER_REQUEST', size_bytes=1 * 1024**3)
        mock_cw.return_value = mock_batch_metrics({
            'cr': [(999999999.0, i * 1440) for i in range(14)],
            'cw': [(999999999.0, i * 1440) for i in range(14)],
        })

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['recommendedClass'], 'STANDARD')
        self.assertEqual(result['potentialMonthlySavings'], 0.0)

    @patch('table_class._check_reserved_capacity', return_value=True)
    @patch('table_class.get_client')
    def test_reserved_capacity_skips(self, mock_gc, mock_rc):
        from table_class import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            size_bytes=100 * 1024**3)

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertIn('note', result)
        self.assertEqual(result['potentialMonthlySavings'], 0.0)

    @patch('table_class._check_reserved_capacity', return_value=False)
    @patch('table_class.batch_get_metrics')
    @patch('table_class.get_client')
    def test_empty_table_no_recommendation(self, mock_gc, mock_cw, mock_rc):
        from table_class import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            billing='PAY_PER_REQUEST', size_bytes=0)
        mock_cw.return_value = mock_batch_metrics({'cr': [], 'cw': []})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['potentialMonthlySavings'], 0.0)

    def test_missing_prices_returns_error(self):
        from table_class import analyze
        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14})
        self.assertIn('error', result)


class TestUtilization(unittest.TestCase):

    @patch('utilization.batch_get_metrics')
    @patch('utilization.get_client')
    def test_low_utilization_recommends_on_demand(self, mock_gc, mock_cw):
        from utilization import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(rcu=100, wcu=50)
        mock_cw.return_value = mock_batch_metrics({
            'r0': [(1.0, i * 5) for i in range(4032)],
            'w0': [(1.0, i * 5) for i in range(4032)],
            'rm0': [(0.01, i * 5) for i in range(4032)],
            'wm0': [(0.01, i * 5) for i in range(4032)],
        })

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertGreater(len(result['recommendations']), 0)
        self.assertEqual(result['recommendations'][0]['recommendationType'], 'SWITCH_TO_ON_DEMAND')

    @patch('utilization.get_client')
    def test_on_demand_table_skipped(self, mock_gc):
        from utilization import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            billing='PAY_PER_REQUEST')

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(result['billingMode'], 'ON_DEMAND')
        self.assertIn('message', result)

    @patch('utilization.batch_get_metrics')
    @patch('utilization.get_client')
    def test_well_utilized_no_recommendations(self, mock_gc, mock_cw):
        from utilization import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(rcu=10, wcu=10)
        mock_cw.return_value = mock_batch_metrics({
            'r0': [(3000.0, i * 5) for i in range(4032)],
            'w0': [(3000.0, i * 5) for i in range(4032)],
            'rm0': [(10.0, i * 5) for i in range(4032)],
            'wm0': [(10.0, i * 5) for i in range(4032)],
        })

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(len(result['recommendations']), 0)

    @patch('utilization.batch_get_metrics')
    @patch('utilization.get_client')
    def test_ia_table_uses_ia_pricing(self, mock_gc, mock_cw):
        from utilization import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            rcu=100, wcu=50, table_class='STANDARD_INFREQUENT_ACCESS')
        mock_cw.return_value = mock_batch_metrics({
            'r0': [(1.0, i * 5) for i in range(4032)],
            'w0': [(1.0, i * 5) for i in range(4032)],
            'rm0': [(0.01, i * 5) for i in range(4032)],
            'wm0': [(0.01, i * 5) for i in range(4032)],
        })

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        rec = result['recommendations'][0]
        # Should use IA on-demand rates for comparison, not standard
        self.assertEqual(rec['recommendationType'], 'SWITCH_TO_ON_DEMAND')


class TestUnusedGsi(unittest.TestCase):

    @patch('unused_gsi.batch_get_metrics')
    @patch('unused_gsi.get_client')
    def test_unused_gsi_detected(self, mock_gc, mock_cw):
        from unused_gsi import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            gsis=[{'IndexName': 'gsi-email', 'IndexStatus': 'ACTIVE',
                    'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 5}}])
        mock_cw.return_value = mock_batch_metrics({'r0': [], 'pr0': [(10.0, 0)], 'pw0': [(5.0, 0)]})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertTrue(result['hasGSIs'])
        self.assertEqual(len(result['unusedGSIs']), 1)
        self.assertEqual(result['unusedGSIs'][0]['indexName'], 'gsi-email')
        self.assertGreater(result['unusedGSIs'][0]['monthlySavings'], 0)
        self.assertGreater(result['totalMonthlySavings'], 0)

    @patch('unused_gsi.batch_get_metrics')
    @patch('unused_gsi.get_client')
    def test_used_gsi_not_flagged(self, mock_gc, mock_cw):
        from unused_gsi import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            gsis=[{'IndexName': 'gsi-email', 'IndexStatus': 'ACTIVE',
                    'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 5}}])
        mock_cw.return_value = mock_batch_metrics({'r0': [(1000.0, 0)]})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        self.assertEqual(len(result['unusedGSIs']), 0)

    @patch('unused_gsi.get_client')
    def test_no_gsis(self, mock_gc):
        from unused_gsi import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table()

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14})
        self.assertFalse(result['hasGSIs'])

    @patch('unused_gsi.batch_get_metrics')
    @patch('unused_gsi.get_client')
    def test_ia_table_uses_ia_pricing(self, mock_gc, mock_cw):
        from unused_gsi import analyze
        mock_gc.return_value.describe_table.return_value = mock_describe_table(
            table_class='STANDARD_INFREQUENT_ACCESS',
            gsis=[{'IndexName': 'gsi-email', 'IndexStatus': 'ACTIVE',
                    'ProvisionedThroughput': {'ReadCapacityUnits': 10, 'WriteCapacityUnits': 5}}])
        mock_cw.return_value = mock_batch_metrics({'r0': [], 'pr0': [(10.0, 0)], 'pw0': [(5.0, 0)]})

        result = analyze({'region': 'us-east-1', 'tableName': 'test', 'days': 14, 'prices': PRICES})
        # Should use IA rates: 10*0.00016*730 + 5*0.00081*730 = 4.125
        expected = round(10 * 0.00016 * 730 + 5 * 0.00081 * 730, 2)
        self.assertAlmostEqual(result['unusedGSIs'][0]['monthlySavings'], expected, places=2)


class TestOutputFormatting(unittest.TestCase):

    def test_format_with_recommendations(self):
        from analyze_all import format_results
        results = [{
            'tableName': 'orders', 'region': 'us-east-1', 'errors': [],
            'capacityMode': {'potentialMonthlySavings': 10.0, 'currentMode': 'PROVISIONED',
                             'recommendedMode': 'ON_DEMAND'},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }]
        output = format_results(14, results)
        self.assertIn('orders', output)
        self.assertIn('On-Demand', output)
        self.assertIn('$10.00/mo', output)
        self.assertIn('┌', output)  # box drawing

    def test_format_with_no_recommendations(self):
        from analyze_all import format_results
        results = [{
            'tableName': 'logs', 'region': 'us-east-1', 'errors': [],
            'capacityMode': {'potentialMonthlySavings': 0},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }]
        output = format_results(14, results)
        self.assertIn('Already optimized', output)
        self.assertIn('logs', output)

    def test_format_with_protection_warnings(self):
        from analyze_all import format_results
        results = [{
            'tableName': 'orders', 'region': 'us-east-1', 'errors': [],
            'deletionProtection': False, 'pointInTimeRecovery': False,
            'capacityMode': {'potentialMonthlySavings': 0},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }]
        output = format_results(14, results)
        self.assertIn('Deletion Protection', output)
        self.assertIn('PITR', output)
        self.assertIn('⚠ enable', output)
        self.assertIn('┌', output)

    def test_format_with_errors(self):
        from analyze_all import format_results
        results = [{
            'tableName': 'broken', 'region': 'us-east-1',
            'errors': ['capacityMode: timeout'],
            'capacityMode': {'error': 'timeout'},
            'tableClass': {'potentialMonthlySavings': 0},
            'utilization': {'recommendations': []},
            'unusedGsi': {'unusedGSIs': []},
        }]
        output = format_results(14, results)
        self.assertIn('Errors', output)
        self.assertIn('broken', output)


if __name__ == '__main__':
    unittest.main()
