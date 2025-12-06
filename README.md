# Basketball Data Pipeline

This repository implements a medallion-style (bronze–silver–gold) data pipeline for basketball data using Python, HiveQL, and Jenkins for automation.[file:2]

## Overview

The project ingests raw basketball-related data, stages it into silver tables, and then transforms it into gold analytical tables suitable for reporting and downstream analytics.[file:2] Orchestration and CI/CD are handled via a Jenkins pipeline defined in the `Jenkinsfile` on the `Development-Branch`.[file:2]

## Key Components

- **HiveQl/**  
  Contains HiveQL scripts for creating and maintaining bronze, silver, and gold layer tables.[file:2]

- **fact_dim_gold/**  
  Holds DDL/DML for fact and dimension tables in the gold layer, including recent modifications to fact/dim structures.[file:2]

- **ETL / Transformation Scripts (Python)**  
  - `games.py` – Processes game-level data and prepares it for silver/gold tables.[file:2]  
  - `silver_games.py`, `silver_players.py`, `silver_playerstats.py`, `silver_teamhistories.py`, `silver_teamstatistics.py` – Silver-layer transformations by entity (games, players, player stats, team histories, team stats).[file:2]  
  - `gold_win_pct.py` – Gold-layer script that derives win-percentage style metrics from curated tables.[file:2]  
  - `incremental-load.py` – Implements incremental load logic to avoid full reloads of source data.[file:2]  
  - `silver-to-gold.py` – Promotes curated silver data into gold fact/dim structures.[file:2]  
  - `loacl_to_postgress.py` – Moves data from a local environment into PostgreSQL as part of the ingestion workflow.[file:2]

- **Jenkinsfile**  
  Defines the Jenkins pipeline used to run linting, tests, and scheduled or on-demand ETL jobs against the Development branch.[file:2]

## Getting Started

### Prerequisites

- Python 3.x (with common data libraries such as `pandas` and database connectors, to be specified per environment).  
- Hive / Hadoop environment for running HiveQL scripts.  
- PostgreSQL instance for staging or serving data.  
- Jenkins or another CI/CD tool capable of interpreting the provided `Jenkinsfile`.[file:2]

### Setup

1. Clone the repository:  
git clone https://github.com/winnerstc/Basketball.git
cd Basketball


3. Create and activate a virtual environment, then install project requirements (add a `requirements.txt` or use your environment’s package list).  

4. Configure connection details (PostgreSQL, Hive, etc.) via environment variables or a configuration file used by the Python scripts.

## Running the Pipeline

- Use the Python scripts in the `Development-Branch` to run transformations locally, for example:  
python incremental-load.py
python silver_games.py
python silver-to-gold.py
python gold_win_pct.py

- Configure Jenkins with the `Jenkinsfile` to schedule or trigger the end-to-end pipeline (ingest → silver → gold) on commit or on a cron schedule.[file:2]

## Branching Strategy

- **main** – Stable branch intended for production-ready code.  
- **Development-Branch** – Active development branch where most recent commits (HiveQL changes, silver/gold scripts, Jenkins updates) are made.[file:2]

## Contributions

Contributions are welcome. To contribute:

1. Fork the repository and create a feature branch from `Development-Branch`.  
2. Commit changes with clear messages referencing any related issues or pull requests.  
3. Open a pull request and ensure Jenkins checks pass before requesting review.[file:2]

## License

Add the appropriate license information here (for example, MIT, Apache 2.0, or organization-specific licensing).
