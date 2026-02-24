"""Unused GSI detection - uses GetMetricData batch API.

Usage: echo '{"region":"eu-west-1","tableName":"my-table","days":14,"prices":{...}}' | python unused_gsi.py
"""
import json
import sys
import os
import boto3
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cw_batch import batch_get_metrics
from config import get_client, parse_input, validate_keys
from typing import Any, Dict

def analyze(data: Dict[str, Any]) -> Dict[str, Any]:
    region = data['region']
    table_name = data['tableName']
    days = data.get('days', 14)
    prices = data.get('prices')

    ddb = get_client('dynamodb', region)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    info = ddb.describe_table(TableName=table_name)['Table']
    gsis = info.get('GlobalSecondaryIndexes', [])
    billing = info.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
    is_on_demand = billing == 'PAY_PER_REQUEST'

    if not gsis:
        return {'tableName': table_name, 'hasGSIs': False, 'unusedGSIs': [], 'analysisDays': days}

    queries = []
    for i, gsi in enumerate(gsis):
        queries.append({'id': f'r{i}', 'table': table_name, 'gsi': gsi['IndexName'],
                        'metric': 'ConsumedReadCapacityUnits', 'period': 86400, 'stat': 'Sum'})
        if is_on_demand:
            queries.append({'id': f'w{i}', 'table': table_name, 'gsi': gsi['IndexName'],
                            'metric': 'ConsumedWriteCapacityUnits', 'period': 86400, 'stat': 'Sum'})
        else:
            queries.append({'id': f'pr{i}', 'table': table_name, 'gsi': gsi['IndexName'],
                            'metric': 'ProvisionedReadCapacityUnits', 'period': 86400, 'stat': 'Average'})
            queries.append({'id': f'pw{i}', 'table': table_name, 'gsi': gsi['IndexName'],
                            'metric': 'ProvisionedWriteCapacityUnits', 'period': 86400, 'stat': 'Average'})

    metrics = batch_get_metrics(region, queries, start, now)

    unused = []
    total_savings = 0.0
    for i, gsi in enumerate(gsis):
        total_reads = sum(dp['value'] for dp in metrics.get(f'r{i}', []))
        if total_reads > 0:
            continue

        savings = 0.0
        if prices:
            if is_on_demand:
                total_writes = sum(dp['value'] for dp in metrics.get(f'w{i}', []))
                savings = (total_writes * prices.get('write_request', 0) / days) * 30.4
            else:
                pr = metrics.get(f'pr{i}', [])
                pw = metrics.get(f'pw{i}', [])
                avg_r = sum(dp['value'] for dp in pr) / len(pr) if pr else 0
                avg_w = sum(dp['value'] for dp in pw) / len(pw) if pw else 0
                savings = (avg_r * prices.get('rcu_hour', 0) + avg_w * prices.get('wcu_hour', 0)) * 730

        total_savings += savings
        entry: Dict[str, Any] = {'indexName': gsi['IndexName'], 'monthlySavings': round(savings, 2)}
        unused.append(entry)

    return {
        'tableName': table_name, 'hasGSIs': True,
        'totalGSIs': len(gsis), 'unusedGSIs': unused,
        'totalMonthlySavings': round(total_savings, 2), 'analysisDays': days,
    }

if __name__ == '__main__':
    data = parse_input()
    validate_keys(data, ['region', 'tableName'])
    print(json.dumps(analyze(data), indent=2, default=str))
