UA Aid Intelligence Hub

UA Aid Intelligence Hub is an open-source, high-performance ETL ecosystem designed to aggregate, clean, and analyze the transparency of aid flows to Ukraine. By correlating millions of donation records with global news trends and financial data, it transforms raw fragments into structured intelligence.

Key Features
Massive Data Processing: Engineered to handle 2,000,000+ transaction records with strict deduplication and integrity checks.

Intelligence Correlation: Integrated with Google BigQuery (GDELT Project) to map real-world news events against donation spikes.

Multi-Fund Aggregation: Automated scrapers for United24, Come Back Alive, and other major Ukrainian foundations.

Financial Accuracy: Real-time NBU (National Bank of Ukraine) exchange rate synchronization for precise multi-currency analytics.

Interactive BI: Advanced data modeling and visualization via Apache Superset.

Tech Stack

The project is built on a high-performance Ubuntu-native stack, avoiding virtualization overhead for maximum throughput:

Language: Python 3.11+ (Psycopg2, Cloudscraper, SQLAlchemy, Pandas)

Database: PostgreSQL (Optimized for large-scale relational data)

Cloud: Google BigQuery (Global News Mining)

Operating System: Ubuntu Server (Bare-metal / WSL2 performance)

Visualization: Apache Superset (Enterprise-grade BI)

System Architecture

Extraction: Custom scrapers (PDF & API) bypass anti-bot protections to gather raw data.

Transformation: Python pipelines perform heavy cleaning, deduplication, and ID generation.

Loading: Data is pushed into a strictly structured PostgreSQL schema on Ubuntu.

Analytics: Python and Apache Superset queries the DB to generate real-time, data-driven insights.
