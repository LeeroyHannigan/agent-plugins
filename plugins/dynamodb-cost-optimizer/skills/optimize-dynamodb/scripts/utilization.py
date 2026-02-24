"""Utilization analysis - uses GetMetricData batch API for table + GSIs.

Usage: echo '{"region":"eu-west-1","tableName":"my-table","days":14,"prices":{...}}' | python utilization.py
"""
import json
import sys
import os
import boto3
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cw_batch import batch_get_metrics
from config import UTILIZATION_THRESHOLD, ON_DEMAND_THRESHOLD, get_client, parse_input, validate_keys
from typing import Any, Dict

# Seconds per month (30.4 days) — converts avg units/sec to monthly request units
SECONDS_PER_MONTH: float = 30.4 * 86400  # 2,626,560

# Hours per month for provisioned cost
HOURS_PER_MONTH: int = 730

def analyze(data: Dict[str, Any]) -> Dict[str, Any]:
    region = data['region']
    table_name = data['tableName']
    days = data.get('days', 14)
    prices = data['prices']
    threshold = data.get('utilizationThreshold', UTILIZATION_THRESHOLD)

    ddb = get_client('dynamodb', region)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    info = ddb.describe_table(TableName=table_name)['Table']
    billing = info.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
    if billing == 'PAY_PER_REQUEST':
        return {'tableName': table_name, 'billingMode': 'ON_DEMAND',
                'message': 'Utilization analysis only applies to PROVISIONED tables'}

    # Build queries for table + all GSIs in one batch
    resources = [{
        'name': table_name, 'type': 'TABLE',
        'provR': info['ProvisionedThroughput']['ReadCapacityUnits'],
        'provW': info['ProvisionedThroughput']['WriteCapacityUnits'],
    }]
    for gsi in info.get('GlobalSecondaryIndexes', []):
        resources.append({
            'name': f"{table_name}#{gsi['IndexName']}", 'type': 'GSI',
            'gsi': gsi['IndexName'],
            'provR': gsi.get('ProvisionedThroughput', {}).get('ReadCapacityUnits', 0),
            'provW': gsi.get('ProvisionedThroughput', {}).get('WriteCapacityUnits', 0),
        })

    queries = []
    for i, res in enumerate(resources):
        base = {'table': table_name, 'period': 300}
        if res.get('gsi'):
            base['gsi'] = res['gsi']
        queries.append({**base, 'id': f'r{i}', 'metric': 'ConsumedReadCapacityUnits', 'stat': 'Sum'})
        queries.append({**base, 'id': f'w{i}', 'metric': 'ConsumedWriteCapacityUnits', 'stat': 'Sum'})
        queries.append({**base, 'id': f'rm{i}', 'metric': 'ConsumedReadCapacityUnits', 'stat': 'Maximum'})
        queries.append({**base, 'id': f'wm{i}', 'metric': 'ConsumedWriteCapacityUnits', 'stat': 'Maximum'})

    metrics = batch_get_metrics(region, queries, start, now)

    results = []
    total_savings = 0

    for i, res in enumerate(resources):
        cr = metrics.get(f'r{i}', [])
        cw = metrics.get(f'w{i}', [])
        cr_max = metrics.get(f'rm{i}', [])
        cw_max = metrics.get(f'wm{i}', [])

        avg_r = sum(dp['value'] / 300 for dp in cr) / len(cr) if cr else 0
        avg_w = sum(dp['value'] / 300 for dp in cw) / len(cw) if cw else 0
        max_r = max((dp['value'] for dp in cr_max), default=0)
        max_w = max((dp['value'] for dp in cw_max), default=0)

        r_util = (avg_r / res['provR'] * 100) if res['provR'] > 0 else 0
        w_util = (avg_w / res['provW'] * 100) if res['provW'] > 0 else 0

        if r_util >= threshold and w_util >= threshold:
            continue

        if r_util < ON_DEMAND_THRESHOLD and w_util < ON_DEMAND_THRESHOLD:
            rec_type = 'SWITCH_TO_ON_DEMAND'
            current = (res['provR'] * prices['rcu_hour'] + res['provW'] * prices['wcu_hour']) * HOURS_PER_MONTH
            od = (avg_r * SECONDS_PER_MONTH * prices.get('on_demand_read', prices.get('read_request', 0))) + \
                 (avg_w * SECONDS_PER_MONTH * prices.get('on_demand_write', prices.get('write_request', 0)))
            sav = max(0, current - od)
            rec_r, rec_w = None, None
        else:
            rec_type = 'REDUCE_CAPACITY'
            rec_r = max(5, int(max_r * 1.2)) if r_util < threshold else res['provR']
            rec_w = max(5, int(max_w * 1.2)) if w_util < threshold else res['provW']
            sav = max(0, (res['provR'] - rec_r) * prices['rcu_hour'] * HOURS_PER_MONTH) + \
                  max(0, (res['provW'] - rec_w) * prices['wcu_hour'] * HOURS_PER_MONTH)

        results.append({
            'resourceName': res['name'], 'resourceType': res['type'],
            'readUtilization': round(r_util, 1), 'writeUtilization': round(w_util, 1),
            'recommendationType': rec_type,
            'recommendedRead': rec_r, 'recommendedWrite': rec_w,
            'monthlySavings': round(sav, 2),
        })
        total_savings += sav

    return {
        'tableName': table_name,
        'recommendations': results,
        'totalMonthlySavings': round(total_savings, 2),
        'analysisDays': days,
    }

if __name__ == '__main__':
    data = parse_input()
    validate_keys(data, ['region', 'tableName', 'prices'])
    print(json.dumps(analyze(data), indent=2, default=str))
