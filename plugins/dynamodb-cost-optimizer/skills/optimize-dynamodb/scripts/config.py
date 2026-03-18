"""Shared configuration and utilities for all analyzer scripts."""
import json
import sys
from typing import Any, Dict, List, NoReturn

import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from decimal import Decimal

# Storage-to-throughput cost ratio thresholds (from AWS pricing structure)
# Standard storage: $0.25/GB, Standard-IA storage: $0.10/GB (60% cheaper)
# Standard-IA throughput: ~2.5x more expensive than Standard
# Switch Standard → IA when storage cost dominates throughput cost
STANDARD_TO_IA_RATIO: Decimal = Decimal('0.25') / Decimal('0.6')   # ≈0.417

# Switch IA → Standard when throughput cost dominates storage cost
IA_TO_STANDARD_RATIO: Decimal = Decimal('0.2') / Decimal('1.5')    # ≈0.133

# Utilization below this % triggers right-sizing recommendation
UTILIZATION_THRESHOLD: int = 45

# Below this %, recommend switching to On-Demand entirely
ON_DEMAND_THRESHOLD: int = 30

# Minimum monthly savings to surface a recommendation (USD)
MIN_SAVINGS: Decimal = Decimal('1.0')

# Autoscaling simulation target utilization
AUTOSCALE_TARGET: float = 0.7

# Default analysis window
DEFAULT_DAYS: int = 14

# Maximum analysis window (CloudWatch retains 15 months of data)
MAX_DAYS: int = 90

# Parallel workers for batch analysis
CONCURRENT_WORKERS: int = 10


def get_client(service: str, region: str) -> Any:
    """Create a boto3 client with credential error handling."""
    try:
        return boto3.client(service, region_name=region)
    except NoCredentialsError:
        fail('AWS credentials not configured. Run `aws configure` or set AWS_PROFILE.')
    except ClientError as e:
        fail(f'AWS client error: {e}')


def parse_input() -> Dict[str, Any]:
    """Parse JSON from argv[1] or stdin with validation."""
    try:
        raw = sys.argv[1] if len(sys.argv) > 1 else sys.stdin.read()
        data = json.loads(raw)
    except (json.JSONDecodeError, IndexError) as e:
        fail(f'Invalid JSON input: {e}')
    return data


def validate_keys(data: Dict[str, Any], required: List[str]) -> None:
    """Validate required keys exist in data dict."""
    missing = [k for k in required if k not in data]
    if missing:
        fail(f"Missing required fields: {', '.join(missing)}")
    if 'days' in data:
        data['days'] = max(1, min(int(data['days']), MAX_DAYS))


def get_price_keys(table_info: Dict[str, Any]) -> Dict[str, str]:
    """Return pricing dict keys appropriate for the table's class."""
    tc = table_info.get('TableClassSummary', {}).get('TableClass', 'STANDARD')
    if tc == 'STANDARD_INFREQUENT_ACCESS':
        return {'rcu': 'ia_rcu_hour', 'wcu': 'ia_wcu_hour',
                'read_req': 'ia_read', 'write_req': 'ia_write'}
    return {'rcu': 'rcu_hour', 'wcu': 'wcu_hour',
            'read_req': 'read_request', 'write_req': 'write_request'}


def fail(message: str) -> NoReturn:
    """Print error JSON and exit."""
    print(json.dumps({'error': message}))
    sys.exit(1)
