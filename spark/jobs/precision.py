"""
Precision from hand-labelled backtest samples, with a 95% Wilson confidence interval
(small samples: 40 labels give roughly +-15 points, so report the interval, not just the point).

Accepts either the three original files (label_z.csv, label_market.csv, label_fixed.csv) or the
combined export of the review page (labels_reviewed.csv, which has a `file` column and a
`prelabel` column). For the combined export it also reports:
  - agreement between the human labels and the automatic pre-labels
  - precision without 2025-10-10 (the crash day dominates the baseline sample)

  docker compose exec spark-master python3 /opt/jobs/precision.py /opt/out/labels_reviewed.csv
"""
import csv
import math
import sys
from collections import defaultdict


def wilson(k, n, z=1.96):
    if n == 0:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    r = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - r) / d, (c + r) / d)


def report(name, rows):
    lab = [r for r in rows if r.get("label", "").strip() in ("0", "1")]
    k, n = sum(r["label"].strip() == "1" for r in lab), len(lab)
    lo, hi = wilson(k, n)
    miss = len(rows) - n
    print(f"{name:<34} precision {k}/{n} = {100 * k / max(n, 1):5.1f}%  (95% CI {100 * lo:.1f}-{100 * hi:.1f}%)"
          + (f"  [{miss} unlabelled]" if miss else ""))
    pre = [r for r in lab if r.get("prelabel") in ("0", "1")]
    if pre:
        agree = sum(r["prelabel"] == r["label"].strip() for r in pre)
        print(f"{'':<34} agreement with pre-label: {agree}/{len(pre)} = {100 * agree / len(pre):.0f}%"
              f"   ('unsure' pre-labels: {sum(r.get('prelabel') == 'review' for r in lab)})")


for path in sys.argv[1:]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if rows and "file" in rows[0]:
        groups = defaultdict(list)
        for r in rows:
            groups[r["file"]].append(r)
        print(f"== {path}")
        for g in ("label_z.csv", "label_market.csv", "label_fixed.csv"):
            if g in groups:
                report(g, groups[g])
                no_crash = [r for r in groups[g] if not r.get("detected_minute_utc", "").startswith("2025-10-10")]
                if len(no_crash) != len(groups[g]):
                    report(f"  {g} without 2025-10-10", no_crash)
    else:
        report(path, rows)
