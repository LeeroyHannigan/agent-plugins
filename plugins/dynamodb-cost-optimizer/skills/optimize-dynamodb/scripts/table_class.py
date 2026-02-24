"""Table class analysis - uses GetMetricData batch API.

Usage: echo '{"region":"eu-west-1","tableName":"my-table","days":14,"prices":{...}}' | python table_class.py
"""
import json
import sys
import os
import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from cw_batch import batch_get_metrics
from config import STANDARD_TO_IA_RATIO, IA_TO_STANDARD_RATIO, MIN_SAVINGS, get_client, parse_input, validate_keys
from typing import Any, Dict, Optional

def analyze(data: Dict[str, Any]) -> Dict[str, Any]:
    region = data['region']
    table_name = data['tableName']
    days = data.get('days', 14)
    prices = data.get('prices')
    min_savings = Decimal(str(data.get('minMonthlySavings', MIN_SAVINGS)))

    if not prices:
        return {'tableName': table_name, 'error': 'prices object is required'}

    ddb = get_client('dynamodb', region)
    info = ddb.describe_table(TableName=table_name)['Table']
    current_class = info.get('TableClassSummary', {}).get('TableClass', 'STANDARD')
    size_gb = info.get('TableSizeBytes', 0) / (1024 ** 3)

    reserved = _check_reserved_capacity(region)
    if reserved:
        return {'tableName': table_name, 'currentClass': current_class,
                'recommendedClass': current_class, 'potentialMonthlySavings': 0.0,
                'note': 'Account uses DynamoDB reserved capacity — estimate may differ'}

    note = None
    if reserved is None:
        note = 'Could not verify reserved capacity status — savings estimate may differ'

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    metrics = batch_get_metrics(region, [
        {'id': 'cr', 'table': table_name, 'metric': 'ConsumedReadCapacityUnits', 'period': 86400, 'stat': 'Sum'},
        {'id': 'cw', 'table': table_name, 'metric': 'ConsumedWriteCapacityUnits', 'period': 86400, 'stat': 'Sum'},
    ], start, now)

    total_reads = sum(dp['value'] for dp in metrics.get('cr', []))
    total_writes = sum(dp['value'] for dp in metrics.get('cw', []))

    storage_cost = Decimal(str(size_gb * prices['standard_storage']))
    scale = Decimal('30.4') / Decimal(str(days))
    throughput_cost = (Decimal(str(total_reads * prices['standard_read'])) +
                       Decimal(str(total_writes * prices['standard_write']))) * scale

    total = storage_cost + throughput_cost
    if total == 0:
        return {'tableName': table_name, 'currentClass': current_class,
                'recommendedClass': current_class, 'potentialMonthlySavings': 0.0}

    ratio = storage_cost / throughput_cost if throughput_cost > Decimal('0.01') else Decimal('999.99')

    rec = current_class
    savings = Decimal('0')

    if current_class == 'STANDARD':
        if ratio > STANDARD_TO_IA_RATIO or (throughput_cost <= Decimal('0.01') and storage_cost > Decimal('1.0')):
            proj_s = storage_cost * Decimal('0.4')
            proj_t = throughput_cost * Decimal('2.5')
            savings = total - (proj_s + proj_t)
            rec = 'STANDARD_INFREQUENT_ACCESS' if savings >= min_savings else current_class
            if rec == current_class:
                savings = Decimal('0')
    else:
        if ratio < IA_TO_STANDARD_RATIO:
            proj_s = storage_cost * Decimal('2.5')
            proj_t = throughput_cost * Decimal('0.4')
            savings = total - (proj_s + proj_t)
            rec = 'STANDARD' if savings >= min_savings else current_class
            if rec == current_class:
                savings = Decimal('0')

    result = {
        'tableName': table_name,
        'currentClass': current_class,
        'recommendedClass': rec,
        'monthlyStorageCost': float(storage_cost),
        'monthlyThroughputCost': float(throughput_cost),
        'potentialMonthlySavings': float(savings),
        'storageToThroughputRatio': float(ratio),
        'analysisDays': days,
    }
    if note:
        result['note'] = note
    return result

def _check_reserved_capacity(region: str) -> Optional[bool]:
    """Check for reserved capacity. Returns True/False, or None if check failed."""
    try:
        ce = boto3.client('ce', region_name='us-east-1')
        now = datetime.now(timezone.utc)
        resp = ce.get_cost_and_usage(
            TimePeriod={'Start': (now - timedelta(days=30)).strftime('%Y-%m-%d'),
                        'End': now.strftime('%Y-%m-%d')},
            Granularity='MONTHLY', Metrics=['UnblendedCost'],
            Filter={'And': [
                {'Dimensions': {'Key': 'SERVICE', 'Values': ['Amazon DynamoDB']}},
                {'Dimensions': {'Key': 'REGION', 'Values': [region]}},
            ]},
            GroupBy=[{'Type': 'DIMENSION', 'Key': 'USAGE_TYPE'}],
        )
        for period in resp.get('ResultsByTime', []):
            for group in period.get('Groups', []):
                if 'Commit' in group['Keys'][0]:
                    return True
    except Exception:
        return None
    return False

if __name__ == '__main__':
    data = parse_input()
    validate_keys(data, ['region', 'tableName', 'prices'])
    print(json.dumps(analyze(data), indent=2, default=str))
