# Promo Post Mortem Calculator

A **Streamlit + BigQuery** app for promotion post-mortem analysis. It compares store groups (A/B style) across **Baseline** and **Promo** periods, and outputs copy-friendly tables, waterfall charts, and phase summary text.

---

## 1. Current Capabilities

### 1.1 Store Level (All Categories)
- Pulls store-day traffic and order data from BigQuery and calculates funnel KPIs such as:
  - Traffic & conversion: `pedestrian_footfall`, `incoming_visitors`, `orders`, `store_absorption_rate`, `store_conversion_rate`
  - Business outcomes: `total_revenue`, `total_quantity`, `total_PC1`, `margin`, `AOV`, `price_per_item`
  - Mix KPIs: `RP revenue share`, `promo revenue share`
- Supports flexible store grouping (Group 1 to Group 4), with dynamic Group A / Group B comparison at the top of the page.
- Supports toggling funnel/bridge analysis **including or excluding Sunday**.
- Outputs include:
  - Store Level Funnel tables (copy to Excel)
  - Promo Impact comparison table (A/B delta)
  - PC1 Bridge waterfalls (Baseline / Promo / Baseline→Promo)
  - Weekday KPI bar charts and date-level trend charts

### 1.2 Selected Categories (Deep Dive)
- Supports category-level filtering and drill-down by:
  - `article_section_group`
  - `article_section`
  - `article_season`
  - `article_brand_group`
  - `price_type` (RP/BP)
- Provides category-level Funnel and Promo Impact comparisons.
- Includes multiple waterfall breakdowns (Revenue / Quantity / PC1):
  - Existing Insider vs New+Non Insider
  - Promo vs Non Promo
  - RP vs BP
  - Dimension bridges by section group / section / brand group

### 1.3 Export & Narrative Output
- Export current UI selections (TXT).
- Export raw Store Level and Selected Categories results (CSV).
- Built-in **Phase 1 Driver Summary** text generation and TXT download.
- Table-level “📋 Copy (Excel)” support for direct paste into spreadsheet tools.

---

## 2. End-to-End Workflow (Recommended)

1. **Configure base parameters (Sidebar)**
   - Set traffic/business country and order-side company/channel/country.
   - Set `VAT` and `baseline coefficient`.

2. **Configure store groups (Group 1 to Group 4)**
   - Select all stores, or use Include / Except mode for precise selection.
   - Optionally add group descriptions for easier chart interpretation.

3. **Configure time windows**
   - Baseline and Promo each support:
     - continuous date range, or
     - non-consecutive date selection within a window.

4. **Configure category filters (optional)**
   - Each dimension supports Select All / Include / Except.
   - `price_type` supports RP/BP combinations.

5. **Click Run**
   - Executes BigQuery queries and builds:
     - Store Level (all categories) analysis dataset
     - Selected Categories analysis dataset

6. **Choose comparison groups (Analysis Group A/B)**
   - Dynamically switch the compared groups in the main panel (default Group 1 vs Group 3).

7. **Review outputs and export**
   - Funnel, Promo Impact, waterfall charts, weekday charts, and trend charts.
   - Export CSV/TXT artifacts.
   - Generate and download the Phase 1 summary text.

---

## 3. Data Sources & Metric Notes

- Data source: Google BigQuery.
- Main fact tables include:
  - store traffic table
  - multichannel order data (including `order_items`)
- `baseline coefficient` is applied **only to Baseline dates**, by scaling selected metric columns before downstream aggregations.

> Note: Ensure your runtime has valid credentials and permissions for the target BigQuery project.

---

## 4. Runtime Requirements

- Python 3.10+ (recommended)
- Dependencies in `requirements.txt`:
  - `streamlit`
  - `pandas`
  - `altair`
  - `google-cloud-bigquery`
  - `db-dtypes`
  - `joblib`

Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 5. Run Locally

From the repository root:

```bash
streamlit run src/app.py
```

Then open the local Streamlit URL shown in the terminal (usually `http://localhost:8501`).

---

## 6. Project Structure

```text
promo-postmortem_streamlit/
├── src/
│   ├── app.py                                   # Streamlit UI and orchestration flow
│   ├── store_level_raw_data.py                  # Store-level SQL, data fetch, and KPI aggregation
│   ├── promo_article_section_level_raw_data.py  # Selected-categories SQL and waterfall decomposition
│   ├── report_payload.py                        # Phase 1 payload and summary-text generation
│   ├── sql_builder.py                           # SQL snippet builder helpers
│   └── promo.py                                 # Shared promo analysis utilities
├── requirements.txt
└── README.md
```

---

## 7. FAQ

### Q1: Why do I get no query result?
- Check that:
  - at least one Baseline or Promo date is selected;
  - at least one group (Group 1 to Group 4) contains stores.

### Q2: Why is the store selector empty?
- The app loads store codes from BigQuery using `order_company_name_short + order_channel + order_country`.
- If BigQuery cannot be reached, the app shows a warning and falls back to default store codes.

### Q3: How do I copy tables into Excel?
- Use the “📋 Copy (Excel)” button above each table, then paste directly.

---

## 8. Maintenance Suggestions

- Parameterize BigQuery table names via environment variables or a config file.
- Add automated unit tests for critical KPI aggregation logic.
- Add an offline demo mode (sample data) for environments without cloud access.
