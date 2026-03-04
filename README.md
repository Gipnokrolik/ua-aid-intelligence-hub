UA Aid Intelligence Hub
Real-time ETL Pipeline & Data Analytics for Ukrainian Humanitarian Aid 

Project Vision
The original goal was to aggregate data from a wide range of humanitarian funds for comparative analysis. However, real-world Data Accessibility proved to be a significant challenge: most organizations provide reports with substantial lag (nothing past 2024) or in non-machine-readable formats.

Strategic Pivot:
This project now focuses on high-fidelity, live data from the two most transparent and technologically accessible funds: United24 and Come Back Alive. It demonstrates a complete end-to-end data lifecycle—from custom scraper engineering to interactive BI dashboards.

Tech Stack
Engine: Python 3.12 (running in WSL2 / Ubuntu environment).

Storage: SQLite (Master Database architecture).

ETL & Scrapers: cloudscraper, requests (API integration), and pathlib for robust cross-platform path management.

Analytics & BI: Apache Superset, SQL (Virtual Datasets, Complex Joins, Case Logic).

Automation: Incremental "Live" Scrapers & Historical Backfill processors.

Core Analytical Features
1. Dynamic Currency Normalization
To counter the volatility of the Ukrainian Hryvnia (UAH), I integrated a live link to the National Bank of Ukraine (NBU) exchange rates.

The Result: All donations are dynamically converted to EUR on the fly, allowing for a longitudinal analysis of actual purchasing power across a 3-year timeline.

2. Donor Segmentation (Grassroots vs. Institutional)
I implemented a heuristic segmentation model to distinguish between different donor profiles:

Grassroots (< 500k EUR): High-frequency, individual contributions representing mass support.

Institutional (≥ 500k EUR): Large-scale corporate or foundation transfers.
This allows us to analyze the sustainability of the funds and their reliance on "whales" versus the general public.

3. Incremental ETL Strategy
The system is designed for efficiency. Instead of re-scraping the entire dataset, the "Live" processors check the MAX(date) in the database and only fetch the delta (new records). This minimizes API load and ensures the dashboard stays current with zero manual intervention.

Critical Analysis & Limitations
As a data analyst, I prioritize transparency regarding data integrity:

Categorization Skew: Detailed sector spending (Defense, Medical, Rebuild) is currently sourced only from United24. Come Back Alive data is analyzed as a consolidated pool due to a lack of category-level API reporting.

Aggregated Reporting: Some large entries may represent daily batch reports from foundations rather than single individual transfers.

Looking for Insights?
I am actively looking for additional foundations with open APIs or machine-readable reporting to expand this hub. If you represent an organization or have data leads, let's connect.
