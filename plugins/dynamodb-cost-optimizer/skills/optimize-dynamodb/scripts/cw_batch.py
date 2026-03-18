"""Shared CloudWatch helper using GetMetricData batch API with throttle handling."""
import time
from typing import Any, Dict, List

from botocore.exceptions import ClientError
from datetime import datetime

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_client


def batch_get_metrics(
    region: str,
    queries: List[Dict[str, Any]],
    start: datetime,
    end: datetime,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch multiple metrics in a single GetMetricData call.

    Args:
        region: AWS region
        queries: list of dicts with keys: id, table, metric, period, stat
                 optional: gsi (GSI name)
        start: start datetime
        end: end datetime

    Returns:
        dict mapping query id → list of {timestamp, value}
    """
    cw = get_client('cloudwatch', region)

    metric_queries: List[Dict[str, Any]] = []
    for q in queries:
        dims: List[Dict[str, str]] = [{'Name': 'TableName', 'Value': q['table']}]
        if q.get('gsi'):
            dims.append({'Name': 'GlobalSecondaryIndexName', 'Value': q['gsi']})

        metric_queries.append({
            'Id': q['id'],
            'MetricStat': {
                'Metric': {
                    'Namespace': 'AWS/DynamoDB',
                    'MetricName': q['metric'],
                    'Dimensions': dims,
                },
                'Period': q['period'],
                'Stat': q['stat'],
            },
            'ReturnData': True,
        })

    results: Dict[str, List[Dict[str, Any]]] = {}

    for i in range(0, len(metric_queries), 500):
        batch = metric_queries[i:i+500]
        next_token = None  # type: str | None

        while True:
            params: Dict[str, Any] = {
                'MetricDataQueries': batch,
                'StartTime': start,
                'EndTime': end,
            }
            if next_token:
                params['NextToken'] = next_token

            resp = _call_with_retry(cw.get_metric_data, **params)

            for r in resp.get('MetricDataResults', []):
                qid: str = r['Id']
                if qid not in results:
                    results[qid] = []
                for ts, val in zip(r.get('Timestamps', []), r.get('Values', [])):
                    results[qid].append({'timestamp': ts, 'value': val})

            next_token = resp.get('NextToken')
            if not next_token:
                break

    for qid in results:
        results[qid].sort(key=lambda x: x['timestamp'])

    return results


def _call_with_retry(fn: Any, max_retries: int = 5, **kwargs: Any) -> Dict[str, Any]:
    """Call with exponential backoff on throttling."""
    for attempt in range(max_retries):
        try:
            return fn(**kwargs)
        except ClientError as e:
            code = e.response['Error']['Code']
            if code in ('Throttling', 'ThrottlingException') and attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    return {}  # unreachable, satisfies type checker
