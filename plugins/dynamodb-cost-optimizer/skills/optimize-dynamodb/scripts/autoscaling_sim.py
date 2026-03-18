"""
DynamoDB Autoscaling Simulation.

Simulates how DynamoDB autoscaling would provision capacity based on consumed
capacity metrics, enabling accurate cost comparison between On-Demand and
optimally-autoscaled Provisioned modes.

Autoscaling Rules (per AWS documentation):
- Scale-Out: consumption > target utilization for 2 consecutive minutes
- Scale-In: consumption < (target - 20%) for 15 consecutive minutes
- First 4 scale-ins per day can happen anytime
- After 4 scale-ins, only 1 scale-in per hour
"""
import json
import sys
from typing import List


def simulate(
    metrics: List[float],
    target_utilization: float = 0.7,
    min_cap: int = 1,
    max_cap: int = 40000,
) -> List[float]:
    """
    Simulate autoscaling on a list of per-minute consumed units/sec values.

    Args:
        metrics: consumed units per second, one per minute
        target_utilization: target % (0.7 = 70%)
        min_cap: minimum provisioned capacity
        max_cap: maximum provisioned capacity

    Returns:
        list of simulated provisioned capacity values, one per minute
    """
    n = len(metrics)
    if n == 0:
        return []

    prov: List[float] = [0.0] * n
    prov[0] = max(min_cap, min(metrics[0] / target_utilization, max_cap))

    scale_in_count: int = 0
    last_scale_in_minute: int = -61

    for i in range(1, n):
        prov[i] = prov[i - 1]

        if i % 1440 == 0:
            scale_in_count = 0

        # Scale-out: 2 consecutive minutes above target
        if i >= 2:
            util_prev = metrics[i - 1] / prov[i - 1] if prov[i - 1] > 0 else 0
            util_curr = metrics[i] / prov[i] if prov[i] > 0 else 0
            if util_prev > target_utilization and util_curr > target_utilization:
                needed = max(metrics[i - 1], metrics[i]) / target_utilization
                prov[i] = min(max(needed, prov[i]), max_cap)

        # Scale-in: 15 consecutive minutes below (target - 20%)
        if i >= 15:
            scale_in_threshold: float = target_utilization - 0.2
            all_below: bool = True
            peak: float = 0.0
            for j in range(i - 14, i + 1):
                util = metrics[j] / prov[j] if prov[j] > 0 else 0
                if util >= scale_in_threshold:
                    all_below = False
                    break
                peak = max(peak, metrics[j])

            if all_below:
                can_scale_in = (
                    scale_in_count < 4 or (i - last_scale_in_minute) >= 60
                )
                if can_scale_in:
                    new_cap = max(min_cap, peak / target_utilization)
                    if new_cap * 1.2 < prov[i]:
                        prov[i] = new_cap
                        scale_in_count += 1
                        last_scale_in_minute = i

    return prov


if __name__ == '__main__':
    data = json.loads(sys.stdin.read())
    result = simulate(
        data['metrics'],
        data.get('targetUtilization', 0.7),
        data.get('minCapacity', 1),
        data.get('maxCapacity', 40000),
    )
    print(json.dumps(result))
