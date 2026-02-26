---
name: optimize-dynamodb
description: "Analyze DynamoDB tables for cost optimization opportunities. Triggers on: optimize DynamoDB, DynamoDB cost analysis, reduce DynamoDB costs, DynamoDB capacity mode, on-demand vs provisioned, table class analysis, unused GSI, DynamoDB utilization, right-size DynamoDB."
license: Apache-2.0
dependencies: python>=3.9, boto3
metadata:
  tags: aws, dynamodb, cost-optimization, capacity, provisioned, on-demand, table-class, utilization, gsi
---

# DynamoDB Cost Optimizer

Scripts are fully self-contained — they fetch pricing, metrics, and costs from AWS
via boto3 and return only a small summary. Execute them, do NOT reimplement the logic.

## Prerequisites

Before running any scripts, detect the Python command:

1. Run `python --version`. If it returns Python 3.9+, use `python` for all scripts.
2. If not, try `python3 --version`. If 3.9+, use `python3` for all scripts.
3. If neither works, tell the user to install Python 3.
4. Run `<python> -c "import boto3"`. If it fails, tell the user: `pip install boto3`.
5. AWS credentials configured with: `dynamodb:DescribeTable`, `dynamodb:ListTables`,
   `dynamodb:DescribeContinuousBackups`, `cloudwatch:GetMetricData`, `pricing:GetProducts`,
   `ce:GetCostAndUsage`

## Workflow

### Step 1: Region

Ask user for AWS region(s). Default: `us-east-1`. Supports multiple regions.

### Step 2: Run Analysis

Use the batch script to analyze tables. It auto-discovers all tables when `tables` is omitted.
Pricing is fetched automatically per region — no need to pass it.

Script: `scripts/analyze_all.py`

IMPORTANT: Run the script from the user's current working directory using the absolute path
to the script. This ensures the report is saved locally.

Example:
`python3 /path/to/skill/scripts/analyze_all.py '{"region":"REGION","days":14}'`

All tables in a region:
`{"region":"REGION","days":14}`

Specific tables:
`{"region":"REGION","tables":["table1","table2"],"days":14}`

Multi-region:
`{"regions":{"us-east-1":["t1","t2"],"eu-west-1":["t3"]},"days":14}`

This runs all four analyzers (capacity mode, table class, utilization, unused GSIs)
with parallel execution (10 concurrent by default). One command, one approval.

Individual scripts are also available if the user only wants one type of analysis.
These require a `prices` object — use `scripts/get_pricing.py REGION` to fetch it first:

- `scripts/capacity_mode.py` — Input: `{"region":"REGION","tableName":"TABLE","days":14,"prices":PRICING}`
- `scripts/table_class.py` — Input: `{"region":"REGION","tableName":"TABLE","days":14,"prices":PRICING}`
- `scripts/utilization.py` — Input: `{"region":"REGION","tableName":"TABLE","days":14,"prices":PRICING}`
- `scripts/unused_gsi.py` — Input: `{"region":"REGION","tableName":"TABLE","days":14}`

### Step 3: Present Results

The script outputs a summary line and saves the full report to `dynamodb-cost-report.md`
in the user's current working directory.

DO NOT read or summarize the report file. Simply display the script's output, which
includes the summary and file path. The user can open the file themselves if needed.

After displaying the output, ask if the user wants CLI commands for any recommendations.

### Step 4: Generate Actions

For accepted recommendations:

```bash
# Switch to on-demand
aws dynamodb update-table --table-name TABLE --billing-mode PAY_PER_REQUEST

# Switch to provisioned
aws dynamodb update-table --table-name TABLE --billing-mode PROVISIONED \
  --provisioned-throughput ReadCapacityUnits=RCU,WriteCapacityUnits=WCU

# Change table class
aws dynamodb update-table --table-name TABLE --table-class STANDARD_INFREQUENT_ACCESS

# Delete unused GSI
aws dynamodb update-table --table-name TABLE \
  --global-secondary-index-updates '[{"Delete":{"IndexName":"GSI_NAME"}}]'
```

DO NOT execute update commands without explicit user confirmation.

## Error Handling

- Script fails → show error output, DO NOT reimplement logic.
- Reserved capacity detected → table class script handles this, reports it.
- ON_DEMAND table → utilization script handles this, reports it.
- CloudWatch throttling → scripts retry with exponential backoff (up to 5 retries).
- Per-table errors → reported in the output, other tables still analyzed.
- AWS credentials missing → scripts exit with clear error message.
