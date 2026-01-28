## Footnotes — processing, cleaning, reporting

Scope
- NYC TLC yellow taxi trips (green not loaded in this run).

Tech stack
- Ingestion/processing: Bruin pipelines materializing into MotherDuck (DuckDB).
- Viz: Streamlit + Altair.

Cleaning/filters (applied in reports)
- Borough validity: exclude `Unknown`, `N/A`, `Outside of NYC`.
- Distance: 0.05–100 mi.
- Duration: 60–7,200 seconds (1–120 minutes).
- Speed: 1–80 mph.
- Fare: $0–$500; fare_amount > 0.
- Tips: tip_amount ≥ 0; tip_rate (tip_amount / fare_amount) ≤ 100%.
- Non-zero denoms enforced via NULLIF.

Reporting/metrics
- Average tip % by day-of-week x month.
- Average tip % by day-of-week x hour.
- Average tip % by borough pickup→dropoff flow.
- Monthly totals: trips, fare_sum, tip_sum, total_sum + tip_rate trend.

Row-removal counts (from prior profiling; overlap exists)
- Unknown/ambiguous boroughs: ~780k (unknown) + ~523k (N/A/outside).
- Distance outside bounds: ~2.73M.
- Duration outside bounds: ~0.97M.
- Fare outside bounds: 1,826.
- Negative tips: 15.
- Speed outside bounds: ~2.71M.

Notes:
- Counts are computed independently per condition to show potential impact; overlaps mean total removed after all filters is less than the sum.
- These filters are enforced in the reports (monthly shocks, hourly tipping, route extremes, socio trends, seasonality, borough flows, monthly totals).