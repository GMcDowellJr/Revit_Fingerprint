from __future__ import annotations

from typing import List


def jenks_breaks(values: list, n_classes: int = 2) -> List[float]:
    """
    Fisher-Jenks natural breaks. Pure Python, no external dependencies.
    Returns a list of (n_classes - 1) break point values.
    For n_classes=2 returns [break_0] — the single split point.
    For n_classes=3 returns [break_0, break_1].
    Values below break_0 are class 1 (lowest). Handles empty or
    constant-value lists gracefully (returns [] or [values[0]]).
    """
    if n_classes < 2:
        raise ValueError("n_classes must be >= 2")
    if not values:
        return []

    vals = sorted(float(v) for v in values)
    unique_count = len(set(vals))
    if unique_count == 1:
        return [vals[0]] * (n_classes - 1)
    if len(vals) <= n_classes:
        if len(vals) == 1:
            return [vals[0]] * (n_classes - 1)
        base = [vals[i] for i in range(1, len(vals))]
        while len(base) < (n_classes - 1):
            base.append(vals[-1])
        return sorted(base[: n_classes - 1])

    n = len(vals)
    lower_class_limits = [[0] * (n_classes + 1) for _ in range(n + 1)]
    variance_combinations = [[float("inf")] * (n_classes + 1) for _ in range(n + 1)]

    for i in range(1, n_classes + 1):
        lower_class_limits[1][i] = 1
        variance_combinations[1][i] = 0.0
        for j in range(2, n + 1):
            variance_combinations[j][i] = float("inf")

    for l in range(2, n + 1):
        sum_ = 0.0
        sum_squares = 0.0
        w = 0
        variance = 0.0
        for m in range(1, l + 1):
            lower_class_limit = l - m + 1
            val = vals[lower_class_limit - 1]
            w += 1
            sum_ += val
            sum_squares += val * val
            variance = sum_squares - (sum_ * sum_) / w
            if lower_class_limit != 1:
                for j in range(2, n_classes + 1):
                    test_variance = variance + variance_combinations[lower_class_limit - 1][j - 1]
                    if variance_combinations[l][j] >= test_variance:
                        lower_class_limits[l][j] = lower_class_limit
                        variance_combinations[l][j] = test_variance

        lower_class_limits[l][1] = 1
        variance_combinations[l][1] = variance

    breaks = [0.0] * (n_classes + 1)
    breaks[n_classes] = vals[-1]
    breaks[0] = vals[0]

    k = n
    for j in range(n_classes, 1, -1):
        idx = lower_class_limits[k][j] - 1
        breaks[j - 1] = vals[idx]
        k = lower_class_limits[k][j] - 1

    return sorted(breaks[1:-1])
