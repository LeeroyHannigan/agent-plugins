"""Fetch all DynamoDB pricing for a region via boto3 Pricing API."""
import json
import sys
import os
from typing import Any, Dict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import get_client

def get_pricing(region: str) -> Dict[str, float]:
    """Fetch DynamoDB pricing. Pricing API is always in us-east-1."""
    pricing = get_client('pricing', 'us-east-1')
    prices: Dict[str, float] = {}

    for family, mappings in [
        ('Amazon DynamoDB PayPerRequest Throughput', {
            'DDB-WriteUnits': 'write_request',
            'DDB-ReadUnits': 'read_request',
            'DDB-WriteUnitsIA': 'ia_write',
            'DDB-ReadUnitsIA': 'ia_read',
        }),
        ('Provisioned IOPS', {
            'WriteCapacityUnit-Hrs': 'wcu_hour',
            'ReadCapacityUnit-Hrs': 'rcu_hour',
        }),
        ('Database Storage', {}),
    ]:
        next_token = None
        while True:
            params: Dict[str, Any] = {
                'ServiceCode': 'AmazonDynamoDB',
                'Filters': [
                    {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region},
                    {'Type': 'TERM_MATCH', 'Field': 'productFamily', 'Value': family},
                ],
                'MaxResults': 100,
            }
            if next_token:
                params['NextToken'] = next_token

            resp = pricing.get_products(**params)

            for item in resp['PriceList']:
                data = json.loads(item)
                attrs = data.get('product', {}).get('attributes', {})
                group = attrs.get('group', '')
                usage = attrs.get('usagetype', '')
                vol = attrs.get('volumeType', '')

                for term in data.get('terms', {}).get('OnDemand', {}).values():
                    for dim in term.get('priceDimensions', {}).values():
                        p = float(dim['pricePerUnit']['USD'])
                        if p <= 0:
                            continue

                        # On-demand / provisioned
                        for key, name in mappings.items():
                            if (key in group or key in usage) and name not in prices:
                                prices[name] = p

                        # Storage
                        if family == 'Database Storage':
                            if '- IA' in vol and 'ia_storage' not in prices:
                                prices['ia_storage'] = p
                            elif '- IA' not in vol and 'standard_storage' not in prices:
                                prices['standard_storage'] = p

            next_token = resp.get('NextToken')
            if not next_token:
                break

    required = ['read_request', 'write_request', 'rcu_hour', 'wcu_hour', 'standard_storage']
    missing = [k for k in required if k not in prices]
    if missing:
        from config import fail
        fail(f"Could not fetch pricing for: {', '.join(missing)} in {region}")

    # Aliases
    prices.setdefault('standard_read', prices['read_request'])
    prices.setdefault('standard_write', prices['write_request'])
    prices.setdefault('on_demand_read', prices['read_request'])
    prices.setdefault('on_demand_write', prices['write_request'])

    return prices

if __name__ == '__main__':
    region = sys.argv[1] if len(sys.argv) > 1 else 'us-east-1'
    print(json.dumps(get_pricing(region), indent=2))
