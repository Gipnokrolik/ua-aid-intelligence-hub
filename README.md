UA Aid Intelligence Hub is an open-source, high-performance ETL ecosystem designed to aggregate, clean, and analyze the transparency of aid flows to Ukraine. By correlating millions of donation records with global news trends and financial data, it transforms raw fragments into structured intelligence.

Key Features

Autonomous Orchestration: Managed by Apache Airflow, ensuring 24/7 reliability with scheduled DAGs and automated retry logic.

Massive Data Processing: Engineered to handle 2,000,000+ transaction records with strict deduplication and integrity checks.

Advanced Statistical Analysis: Deep Exploratory Data Analysis (EDA), time-series anomaly detection, and hypothesis testing (e.g., Mann-Whitney U test, Welch's t-test) to identify non-obvious correlations between "whale" donor behavior and global events.

Intelligence Correlation: Integrated with Google BigQuery (GDELT Project) to map global news events against donation spikes.

Real-time Telemetry: Automated Telegram Reporting system powered by Airflow XComs, providing instant updates on data ingestion metrics and pipeline health.

Scalable Multi-Fund Aggregation: High-resilience scrapers with anti-bot bypass capabilities currently process data from United24 and Come Back Alive. While the architecture was designed for a massive multi-fund scope, severe inconsistencies and poor reporting standards across most NGOs have temporarily limited the active pipeline to these two verified sources. The system is built to scale, and active scouting for new foundations meeting strict transparency criteria is ongoing.

Financial Accuracy: Real-time NBU (National Bank of Ukraine) exchange rate synchronization for precise multi-currency analytics.

Tech Stack
The project is built on a high-performance, production-ready stack:

Orchestration: Apache Airflow 2.x (DAGs, XCom, Callbacks)

Data Engineering: Python (Psycopg2, Cloudscraper, SQLAlchemy, Selenium)

Data Science & EDA: Jupyter Notebooks, Pandas, NumPy, SciPy, Pingouin, Statsmodels, Seaborn

Database: PostgreSQL (Optimized for large-scale relational data)

Cloud: Google BigQuery (Global News Mining)

Alerting: Telegram Bot API (REST-based monitoring)

Environment: Ubuntu Server / WSL2 (High-performance Linux kernel)

Visualization: Apache Superset (Enterprise-grade BI)

System Architecture

Orchestration Layer: Airflow triggers daily extraction cycles, managing dependencies between financial, news, and donation tasks.

Extraction: Custom scrapers (PDF, API, and Headless Chrome) bypass protections to gather raw data from distributed sources.

Transformation: Python pipelines perform heavy cleaning, cross-currency normalization, deduplication, and deterministic ID generation.

Loading: Validated data is pushed into a strictly structured PostgreSQL schema using ACID-compliant transactions.

Telemetry: Upon task completion, metadata (row counts, execution time) is aggregated and dispatched via the Telegram Bot API.

Analytics & Insights: Apache Superset queries the master DB to generate real-time interactive dashboards. Concurrently, deep-dive analytics—including distribution modeling, statistical hypothesis testing, and NLP-based entity extraction from unstructured comments (Entity Resolution)—are conducted in connected Jupyter Notebook environments. This enables the precise segmentation of completely ID-less transaction streams, isolating grassroots micro-donations from corporate B2B "whales" to analyze their distinct behavioral patterns and extract actionable insights.

Deployment: The hub utilizes a decoupled architecture where scrapers run in isolated virtual environments managed by the Airflow scheduler, ensuring maximum stability and dependency control.
