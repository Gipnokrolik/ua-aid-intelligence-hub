UA Aid Intelligence Hub

UA Aid Intelligence Hub is an open-source, high-performance ETL ecosystem designed to aggregate, clean, and analyze the transparency of aid flows to Ukraine. By correlating millions of donation records with global news trends and financial data, it transforms raw fragments into structured intelligence.

🚀 Key Features
Autonomous Orchestration: Managed by Apache Airflow, ensuring 24/7 reliability with scheduled DAGs and automated retry logic.

Massive Data Processing: Engineered to handle 2,000,000+ transaction records with strict deduplication and integrity checks.

Intelligence Correlation: Integrated with Google BigQuery (GDELT Project) to map global news events against donation spikes.

Real-time Telemetry: Automated Telegram Reporting system powered by Airflow XComs, providing instant updates on data ingestion metrics and pipeline health.

Multi-Fund Aggregation: High-resilience scrapers for United24, Come Back Alive, and other major foundations with anti-bot bypass capabilities.

Financial Accuracy: Real-time NBU (National Bank of Ukraine) exchange rate synchronization for precise multi-currency analytics.

🛠 Tech Stack
The project is built on a high-performance, production-ready stack:

Orchestration: Apache Airflow 2.x (DAGs, XCom, Callbacks)

Language: Python 3.11+ (Psycopg2, Cloudscraper, SQLAlchemy, Pandas, Selenium)

Database: PostgreSQL (Optimized for large-scale relational data)

Cloud: Google BigQuery (Global News Mining)

Alerting: Telegram Bot API (REST-based monitoring)

Environment: Ubuntu Server / WSL2 (High-performance Linux kernel)

Visualization: Apache Superset (Enterprise-grade BI)

🏗 System Architecture
Orchestration Layer: Airflow triggers daily extraction cycles, managing dependencies between financial, news, and donation tasks.

Extraction: Custom scrapers (PDF, API, and Headless Chrome) bypass protections to gather raw data from distributed sources.

Transformation: Python pipelines perform heavy cleaning, cross-currency normalization, deduplication, and deterministic ID generation.

Loading: Validated data is pushed into a strictly structured PostgreSQL schema using ACID-compliant transactions.

Telemetry: Upon task completion, metadata (row counts, execution time) is aggregated and dispatched via the Telegram Bot API.

Analytics: Apache Superset queries the master DB to generate real-time, data-driven insights and interactive dashboards.

📦 Deployment
The hub utilizes a decoupled architecture where scrapers run in isolated virtual environments managed by the Airflow scheduler, ensuring maximum stability and dependency control.
