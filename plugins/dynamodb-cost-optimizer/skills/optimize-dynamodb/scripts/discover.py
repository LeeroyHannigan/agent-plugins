"""DynamoDB table discovery.

Usage:
  python discover.py REGION                          # all tables
  python discover.py REGION my-table                 # single table
  python discover.py REGION table-1 table-2 table-3  # specific tables
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_client
from typing import Any, Dict, List, Optional

def discover(region: str, table_names: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    ddb = get_client('dynamodb', region)

    if table_names is None:
        table_names = []
        for page in ddb.get_paginator('list_tables').paginate():
            table_names.extend(page['TableNames'])

    tables = []
    for name in table_names:
        try:
            t = ddb.describe_table(TableName=name)['Table']
            billing = t.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
            if billing == 'PAY_PER_REQUEST':
                billing = 'ON_DEMAND'
            pitr = False
            try:
                cb = ddb.describe_continuous_backups(TableName=name)
                pitr = cb.get('ContinuousBackupsDescription', {}).get(
                    'PointInTimeRecoveryDescription', {}).get('PointInTimeRecoveryStatus') == 'ENABLED'
            except Exception:
                pass
            tables.append({
                'tableName': name,
                'billingMode': billing,
                'tableClass': t.get('TableClassSummary', {}).get('TableClass', 'STANDARD'),
                'deletionProtection': t.get('DeletionProtectionEnabled', False),
                'pointInTimeRecovery': pitr,
                'itemCount': t.get('ItemCount', 0),
                'tableSizeBytes': t.get('TableSizeBytes', 0),
                'provisionedRead': t.get('ProvisionedThroughput', {}).get('ReadCapacityUnits', 0),
                'provisionedWrite': t.get('ProvisionedThroughput', {}).get('WriteCapacityUnits', 0),
                'gsiCount': len(t.get('GlobalSecondaryIndexes', [])),
            })
        except Exception as e:
            tables.append({'tableName': name, 'error': str(e)})
    return tables

if __name__ == '__main__':
    region = sys.argv[1] if len(sys.argv) > 1 else 'us-east-1'
    names = sys.argv[2:] if len(sys.argv) > 2 else None
    result = discover(region, names)
    print(json.dumps({'tables': result, 'count': len(result)}, indent=2))
