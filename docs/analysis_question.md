# Analysis question

> **Using live and historical availability for ~2,000 Singapore HDB carparks, can we predict whether a carpark will be
> nearly full (≤ 10% of car lots free) 30 minutes from now, and which hours and carpark types are most at risk?**

## Problem
Drivers circle looking for a space, wasting time and fuel and adding congestion. Availability changes minute by minute
and differs between carparks a few hundred metres apart. Singapore publishes live occupancy for its public-housing (HDB)
carparks, but a live number only says what is free **now**, not what will be free **when you arrive**.

## Decisions supported
1. **Driver:** "Head to carpark X, or to nearby carpark Y, which is predicted to have space when I arrive?"
2. **Planner:** "Which carparks and hours are chronically full?" — for pricing, signage and capacity planning.

## Target
`full_30 = 1` if `lots_available / total_lots ≤ 0.10` at t + 30 min for car lots (lot type C), built with a timestamp
self-join on the 15-minute slot grid. The class balance and the threshold decision are in `data_quality_report.md`.

## Success criteria
* Report accuracy, precision, recall and F1 (plus ROC-AUC / PR-AUC) for both models and both baselines on held-out
  **later days** (time-based split).
* Persistence ("full in 30 min if full now") is a strong baseline because the state rarely changes within 30 minutes.
  The model must beat it overall **or** show clear value on the rows where the state changes (transition metrics); either
  outcome is reported honestly.
* Verify predictions on genuinely unseen live data (`live_eval.csv`).

## Why Big Data / Spark
~2,000 carparks × 96 snapshots/day × 30 days ≈ 5.8M snapshot lines (more lot-type records after the flatMap), plus a live
stream every minute. The work involves wide aggregations (shuffles), joins with metadata and time-window features.

## Why Singapore
Indian cities do not publish open, live, machine-readable parking data. Singapore's government feed (data.gov.sg, HDB)
is official, free, live and historical. The method transfers to any city with such a feed.

## Assumptions and limitations
* 30 days of history (one month, no public holidays inside the window) — seasonality beyond a month is not captured.
* HDB carparks only (no shopping-mall or private carparks); some sensors are stale or frozen and are excluded.
* The live source updates every minute but individual sensors can lag (staleness filter: 30 min).
