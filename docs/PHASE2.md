# ScoutPro phase 2 (deferred)

Implement **only after** MiLB→MLB arrival models are trained on a leakage-safe feature version (v3+), evaluated with temporal holdouts, and wired to inference with acceptable calibration.

## A. Time-to-MLB

- **Target:** seasons or years from first MiLB season to MLB debut.
- **Approaches:** regression on labeled `label_years_to_mlb`, or survival analysis for censored careers (`label_censored`).

## B. Similar players

- **Approach:** k-nearest neighbors or learned embeddings on `engineered_features`, using the same feature version as arrival models.
- **Output:** top-k comps with distance and shared drivers (see `ml/comparison_engine.py`).

## C. Earnings / salary projection

- **Prerequisite:** populated `salary_history` and reliable `label_peak_salary_usd` / career earnings labels.
- **Approach:** regression or quantile models per service-time bucket; only ship when label coverage is documented on the Data & Models page.
