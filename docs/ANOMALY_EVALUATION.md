# Anomaly detector evaluation (step 4)

## What is measured
- **Precision**: of the events the detector raises, how many a human reviewer agrees are real anomalies. Measured on a random sample of 40 events per rule, reported with a 95% confidence interval.
- **Recall on known events**: for documented market events (`spark/jobs/known_events.csv`), how many affected symbols the detector flagged inside the event window.
- **Volume and spread**: events per day, events per symbol per day, large caps vs small caps. This tests the claim that a fixed % rule is biased toward small caps.
- **Baseline comparison**: the same numbers for the original project rule (volume ≥ 5x AND |price change| ≥ 4% in 5 min).

Not measured: overall recall (we can't know every real anomaly), and profitability (out of scope by design).

## Labeling guide (fill `label` in label_z.csv / label_fixed.csv)
For each row, open the symbol on a chart with 1-minute candles around `detected_minute_utc`
(Binance web or TradingView, chart time zone set to UTC), and look at about 6 hours around the event.

`label = 1` (real anomaly) if **all** of these hold:
1. The price move in the flagged 5 minutes clearly stands out from the surrounding hours. It is one of the largest moves of that day for this coin.
2. Volume in those minutes is visibly elevated compared to the surrounding hours.
3. It isn't a data artifact: no single-candle wick that reverts within the same minute on tiny volume, and no stale/empty market.

`label = 0` otherwise. Common 0 cases: a slow drift that only crossed the threshold, a move that is normal for a very volatile day, a 1–2 tick change on a micro-priced coin.

Write a few words in `notes` for every 0 (why) and for interesting 1s (e.g. "market-wide drop", "listing news"). These notes become the "error analysis" section of the report.

**Bias control:** label without looking at which rule produced the row, if possible. Each team member labels the same file, and disagreements are discussed. Agreement rate is worth reporting.

## Commands
```bash
docker compose exec spark-master /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g /opt/jobs/backtest.py --months 2025-10,2026-10
# results in ./data/backtest/ on the host; open label_*.csv in Excel, fill `label`, save as CSV
docker compose exec spark-master python3 /opt/jobs/precision.py /opt/out/label_z.csv /opt/out/label_fixed.csv
```

## Tuning
The parameters are environment variables (`AD_*`, see `spark/jobs/anomaly_rules.py`). Tune on one month and report on another month that wasn't used for tuning. Otherwise precision is overstated.

## How the labels were actually collected (2026-11-25)
1. `spark/jobs/label_context.py` extracted the 1-minute klines around every sampled event (its whole UTC day ± 3 h) from the archive cache → `data/backtest/label_context.csv`.
2. `tools/prelabel.py` computed **automatic pre-labels** from full-day context, which the live detector never sees (it only looks back 60 min):
   - **C1 move stands out:** the event's |5-min log return| is ≥ the 99th percentile of that coin's 5-min moves that UTC day **and** ≥ 5× the day's median 5-min move, **or** ≥ 15× the median (on extreme days like 10/10 later moves push early ones below p99).
   - **C2 volume elevated:** 5-min USDT volume ≥ 5× the day's median 5-min volume.
   - **Artifact:** fewer than 20 trades in the 5 minutes, or < 20% of the move left 5 min later on < 3× median volume.
   - pre-label `1` = C1 and C2 and not artifact; `0` = clearly fails; otherwise `review` (shown as "unsure").
   Result: label_z 35×1 / 1×0 / 4 unsure, label_market 16×1, label_fixed 39×1 / 1 unsure.
3. **Pre-labels are not the precision.** C1/C2 overlap with what the detector already demands (high volume ratio, large z), so they mostly confirm that the event is extreme for its day. They can't judge whether it's *meaningful* (e.g. a 1% wiggle on thin volume that a person would dismiss). Precision comes from the human labels; the pre-labels give a second number: **human/rule agreement**.
4. Humans label on the review page (claude.ai artifact "MarketPulse label review"): one chart per event (6 h of 1-min price + volume, flagged window in orange), 1/0 buttons, note field. **Blind mode** (on by default) hides the pre-label until the reviewer has voted, to avoid anchoring. Each reviewer's votes are stored separately (`reviews/<user id>`), so inter-reviewer agreement can be measured.
5. Export: the page's "Download my labels (CSV)" → `data/backtest/labels_reviewed.csv`, then `precision.py labels_reviewed.csv` (precision with CI per set, agreement with pre-labels, and the baseline without the crash day, which is 28 of its 40 sampled events).
