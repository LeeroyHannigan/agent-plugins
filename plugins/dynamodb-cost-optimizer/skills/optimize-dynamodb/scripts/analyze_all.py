"""Batch analyzer - runs all analyzers for multiple tables in parallel with formatted output.

Usage: echo '{"region":"eu-west-1","tables":["t1","t2"],"days":14,"prices":{...}}' | python analyze_all.py

Multi-region: echo '{"regions":{"eu-west-1":["t1"],"us-east-1":["t2"]},"days":14,"prices":{...}}' | python analyze_all.py
Optional: "concurrency": 10 (default 10)
"""
import json
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from capacity_mode import analyze as analyze_capacity
from table_class import analyze as analyze_table_class
from utilization import analyze as analyze_utilization
from unused_gsi import analyze as analyze_unused_gsi
from get_pricing import get_pricing

MODE_LABELS = {'ON_DEMAND': 'On-Demand', 'PROVISIONED': 'Provisioned'}
CLASS_LABELS = {'STANDARD': 'Standard', 'STANDARD_INFREQUENT_ACCESS': 'Standard-IA'}

def analyze_table(region: str, table_name: str, days: int, prices: Dict[str, float]) -> Dict[str, Any]:
    entry = {'tableName': table_name, 'region': region, 'errors': []}
    for key, fn, inp in [
        ('capacityMode', analyze_capacity, {'region': region, 'tableName': table_name, 'days': days, 'prices': prices}),
        ('tableClass', analyze_table_class, {'region': region, 'tableName': table_name, 'days': days, 'prices': prices}),
        ('utilization', analyze_utilization, {'region': region, 'tableName': table_name, 'days': days, 'prices': prices}),
        ('unusedGsi', analyze_unused_gsi, {'region': region, 'tableName': table_name, 'days': days, 'prices': prices}),
    ]:
        try:
            entry[key] = fn(inp)
        except Exception as e:
            entry[key] = {'error': str(e)}
            entry['errors'].append(f"{key}: {e}")
    return entry

def format_results(days: int, results: List[Dict[str, Any]]) -> str:
    recs = []
    optimized = []
    errors = []
    total_savings = 0.0

    for t in results:
        name = t['tableName']
        region = t.get('region', '')
        label = f"{name} ({region})" if len(set(r.get('region', '') for r in results)) > 1 else name

        if t['errors']:
            errors.append({'table': label, 'errors': t['errors']})

        table_recs = []

        cm = t.get('capacityMode', {})
        if cm.get('potentialMonthlySavings', 0) > 0:
            table_recs.append({
                'type': 'Billing Mode',
                'change': f"{MODE_LABELS.get(cm['currentMode'], cm['currentMode'])} → {MODE_LABELS.get(cm['recommendedMode'], cm['recommendedMode'])}",
                'savings': cm['potentialMonthlySavings'],
            })

        tc = t.get('tableClass', {})
        if tc.get('potentialMonthlySavings', 0) > 0:
            table_recs.append({
                'type': 'Table Class',
                'change': f"{CLASS_LABELS.get(tc['currentClass'], tc['currentClass'])} → {CLASS_LABELS.get(tc['recommendedClass'], tc['recommendedClass'])}",
                'savings': tc['potentialMonthlySavings'],
            })

        ut = t.get('utilization', {})
        for r in ut.get('recommendations', []):
            if r.get('monthlySavings', 0) > 0:
                rt = r['recommendationType']
                if rt == 'SWITCH_TO_ON_DEMAND':
                    desc = 'Switch to On-Demand (low utilization)'
                else:
                    desc = f"Right-size (Read: {r.get('recommendedRead')}, Write: {r.get('recommendedWrite')})"
                rtype = f"Utilization ({r['resourceName'].split('#')[-1]})" if r.get('resourceType') == 'GSI' else 'Utilization'
                table_recs.append({'type': rtype, 'change': desc, 'savings': r['monthlySavings']})

        gsi = t.get('unusedGsi', {})
        for g in gsi.get('unusedGSIs', []):
            table_recs.append({
                'type': 'Unused GSI',
                'change': f"Review {g['indexName']} (zero reads in {days} days — verify not needed)",
                'savings': g.get('monthlySavings', 0),
            })

        if table_recs:
            s = sum(r['savings'] for r in table_recs)
            total_savings += s
            recs.append({'table': label, 'recommendations': table_recs, 'totalSavings': s})
        else:
            optimized.append(label)

    recs.sort(key=lambda x: x['totalSavings'], reverse=True)

    regions = sorted(set(r.get('region', '') for r in results))
    region_str = ', '.join(regions) if len(regions) > 1 else regions[0] if regions else ''

    lines = []
    lines.append(f"Region: {region_str} | Analysis: {days} days | Tables: {len(results)} | Savings: ${total_savings:,.2f}/month (${total_savings * 12:,.2f}/year)")
    lines.append("")

    if recs:
        # Calculate column widths
        col_t = max(max((len(t['table']) for t in recs), default=5), 5)
        col_r = max(max((len(f"{r['type']}: {r['change']}") for t in recs for r in t['recommendations']), default=14), 14)
        col_s = 12

        def row(a, b, c):
            return f"│ {a:<{col_t}} │ {b:<{col_r}} │ {c:>{col_s}} │"
        def sep(l, m, r, f='─'):
            return f"{l}{f*(col_t+2)}{m}{f*(col_r+2)}{m}{f*(col_s+2)}{r}"

        lines.append(sep('┌', '┬', '┐'))
        lines.append(row('Table', 'Recommendation', 'Savings'))
        lines.append(sep('├', '┼', '┤'))
        for idx, t in enumerate(recs):
            if idx > 0:
                lines.append(sep('├', '┼', '┤'))
            first = True
            for r in t['recommendations']:
                tname = t['table'] if first else ''
                sav = f"${r['savings']:,.2f}/mo" if r['savings'] > 0 else 'cleanup'
                lines.append(row(tname, f"{r['type']}: {r['change']}", sav))
                first = False
        lines.append(sep('├', '┼', '┤'))
        lines.append(row('TOTAL', '', f"${total_savings:,.2f}/mo"))
        lines.append(sep('└', '┴', '┘'))
        lines.append("")

    if optimized:
        lines.append(f"Already optimized ({len(optimized)}): {', '.join(optimized)}")
        lines.append("")

    if errors:
        lines.append(f"Errors ({len(errors)} tables):")
        for e in errors:
            lines.append(f"  {e['table']}: {'; '.join(e['errors'])}")
        lines.append("")

    return '\n'.join(lines)

def analyze_all(data: Dict[str, Any]) -> str:
    # Support single-region or multi-region input
    if 'regions' in data:
        region_tables = data['regions']
    else:
        region_tables = {data['region']: data['tables']}

    days = data.get('days', 14)
    workers = data.get('concurrency', 10)

    # Auto-fetch pricing per region if not provided
    prices_by_region: Dict[str, Dict[str, float]] = {}
    for region in region_tables:
        if 'prices' in data:
            prices_by_region[region] = data['prices']
        else:
            prices_by_region[region] = get_pricing(region)

    all_tasks = []
    for region, tables in region_tables.items():
        for table in tables:
            all_tasks.append((region, table, prices_by_region[region]))

    results = [None] * len(all_tasks)
    with ThreadPoolExecutor(max_workers=min(workers, len(all_tasks))) as ex:
        futures = {ex.submit(analyze_table, r, t, days, p): i for i, (r, t, p) in enumerate(all_tasks)}
        for f in as_completed(futures):
            results[futures[f]] = f.result()

    return format_results(days, results)

if __name__ == '__main__':
    from config import parse_input, fail
    data = parse_input()
    if 'regions' not in data and 'region' not in data:
        fail("Missing required field: 'region' or 'regions'")
    if 'regions' not in data and 'tables' not in data:
        fail("Missing required field: 'tables'")
    print(analyze_all(data))
