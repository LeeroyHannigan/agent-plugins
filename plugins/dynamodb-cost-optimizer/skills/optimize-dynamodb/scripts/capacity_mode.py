"""Capacity mode analysis - uses GetMetricData batch API.

Usage: echo '{"region":"eu-west-1","tableName":"my-table","days":14,"prices":{...}}' | python capacity_mode.py
"""
import json
import sys
import os
import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from autoscaling_sim import simulate
from cw_batch import batch_get_metrics
from config import get_client, parse_input, validate_keys
from typing import Any, Dict, List

def analyze(data: Dict[str, Any]) -> Dict[str, Any]:
    region = data['region']
    table_name = data['tableName']
    days = data.get('days', 14)
    prices = data['prices']

    ddb = get_client('dynamodb', region)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    info = ddb.describe_table(TableName=table_name)['Table']
    mode = info.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
    if mode == 'PAY_PER_REQUEST':
        mode = 'ON_DEMAND'
    cur_rcu = info.get('ProvisionedThroughput', {}).get('ReadCapacityUnits', 0)
    cur_wcu = info.get('ProvisionedThroughput', {}).get('WriteCapacityUnits', 0)

    # Select pricing keys based on table class
    from config import get_price_keys
    pk = get_price_keys(info)

    # Single batch call for all metrics
    metrics = batch_get_metrics(region, [
        {'id': 'cr', 'table': table_name, 'metric': 'ConsumedReadCapacityUnits', 'period': 300, 'stat': 'Sum'},
        {'id': 'cw', 'table': table_name, 'metric': 'ConsumedWriteCapacityUnits', 'period': 300, 'stat': 'Sum'},
    ], start, now)

    reads = metrics.get('cr', [])
    writes = metrics.get('cw', [])

    total_r = sum(dp['value'] for dp in reads)
    total_w = sum(dp['value'] for dp in writes)

    # On-demand cost
    period_cost = Decimal(str(total_r)) * Decimal(str(prices[pk['read_req']])) + \
                  Decimal(str(total_w)) * Decimal(str(prices[pk['write_req']]))
    od_cost = (period_cost / Decimal(str(days))) * Decimal('30.4')

    # Current provisioned cost
    cur_prov_cost = (Decimal(str(cur_rcu)) * Decimal('730') * Decimal(str(prices[pk['rcu']]))) + \
                    (Decimal(str(cur_wcu)) * Decimal('730') * Decimal(str(prices[pk['wcu']])))

    # Autoscaling simulation
    read_ups = [dp['value'] / 300.0 for dp in reads]
    write_ups = [dp['value'] / 300.0 for dp in writes]
    sim_r = simulate(read_ups) if read_ups else []
    sim_w = simulate(write_ups) if write_ups else []

    if sim_r and sim_w:
        avg_sim_rcu = sum(sim_r) / len(sim_r)
        avg_sim_wcu = sum(sim_w) / len(sim_w)
        optimal_cost = (Decimal(str(avg_sim_rcu)) * Decimal('730') * Decimal(str(prices[pk['rcu']]))) + \
                       (Decimal(str(avg_sim_wcu)) * Decimal('730') * Decimal(str(prices[pk['wcu']])))
    else:
        optimal_cost = cur_prov_cost

    if total_r == 0 and total_w == 0:
        rec = 'ON_DEMAND'
    else:
        rec = 'ON_DEMAND' if od_cost < optimal_cost else 'PROVISIONED'

    current_cost = cur_prov_cost if mode == 'PROVISIONED' else od_cost
    savings = max(Decimal('0'), current_cost - min(od_cost, optimal_cost))

    result = {
        'tableName': table_name,
        'currentMode': mode,
        'recommendedMode': rec,
        'currentMonthlyCost': float(current_cost),
        'onDemandMonthlyCost': float(od_cost),
        'currentProvisionedMonthlyCost': float(cur_prov_cost),
        'optimalProvisionedMonthlyCost': float(optimal_cost),
        'potentialMonthlySavings': float(savings),
        'savingsPercentage': float(savings / current_cost * 100) if current_cost > 0 else 0,
        'analysisDays': days,
    }
    if rec == 'PROVISIONED' and sim_r and sim_w:
        result['recommendedMinRead'] = max(1, int(min(sim_r)))
        result['recommendedMaxRead'] = int(max(sim_r))
        result['recommendedMinWrite'] = max(1, int(min(sim_w)))
        result['recommendedMaxWrite'] = int(max(sim_w))
    return result

if __name__ == '__main__':
    data = parse_input()
    validate_keys(data, ['region', 'tableName', 'prices'])
    print(json.dumps(analyze(data), indent=2, default=str))
