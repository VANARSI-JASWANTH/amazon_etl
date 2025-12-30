# Amazon Order ETL Pipeline with Apache Airflow

An automated ETL (Extract, Transform, Load) pipeline built with Apache Airflow for processing Amazon order data with advanced data quality, transformation, and loading capabilities.

## Features

### Core ETL Pipeline (T0007)
- **Extract**: Reads Amazon order data from CSV/Excel files with automatic format detection
- **Transform**: 10+ data cleaning and transformation operations with config-driven rules
- **Load**: Multi-target output (CSV, Excel, PostgreSQL) with robust error handling

### Data Cleaning & Transformation (T0008-T0017)
- ✅ **T0008**: Reusable cleaning utilities (trim, fillna, typecast)
- ✅ **T0009**: Incorrect data type handling (phone standardization, numeric conversion)
- ✅ **T0010**: Duplicate detection and removal by OrderID
- ✅ **T0011**: Missing data strategies (mean/median fill, forward/backward fill, drop)
- ✅ **T0012**: YAML-driven cleaning rules for flexible configuration
- ✅ **T0013**: Aggregations (groupBy, sum, min, max) for order statistics
- ✅ **T0014**: Normalization & scaling (min-max, standard)
- ✅ **T0015**: Feature engineering (order categories, age groups)
- ✅ **T0016**: Date/time transformations (year, month, day_of_week)
- ✅ **T0017**: Config-based transformation rules via YAML

### Advanced Load Operations (T0018-T0022)
- ✅ **T0018**: Bulk load operations with configurable chunk size (default: 1000 rows)
- ✅ **T0019**: Incremental vs Full load modes
- ✅ **T0020**: Constraint violation handling with row-level isolation
- ✅ **T0021**: Upsert logic for insert-or-skip based on primary keys
- ✅ **T0022**: Reject table for failed records with error context logging

### DAG Orchestration & Workflow Management (T0023-T0027)
- ✅ **T0023**: Master orchestrator DAG for centralized pipeline coordination
- ✅ **T0024**: Event-driven triggering with FileSensor (waits for data file arrival)
- ✅ **T0025**: Multi-DAG dependency management with ExternalTaskSensor
- ✅ **T0026**: Backfill & catchup features for historical data processing
- ✅ **T0027**: Comprehensive failure handling (3 retries, exponential backoff, custom callbacks)

## Data Transformations (Amazon Orders)

1. **Empty/Whitespace Handling**: Converts empty strings to NaN (T0008)
2. **Text Trimming**: Strips leading/trailing whitespace from all string columns (T0008)
3. **Missing Data**: Fills CustomerName with "Unknown", Age with mean (T0011)
4. **Type Handling**: Converts Age to int, standardizes phone numbers (T0009)
5. **Duplicate Removal**: Removes duplicate orders by OrderID (T0010)
6. **Date Processing**: Parses OrderDate, sorts descending, extracts year/month/day (T0016)
7. **Order Aggregations**: Global and state-level statistics (count, sum, min, max) (T0013)
8. **Normalization**: Min-max scaling for TotalAmount (T0014)
9. **Feature Engineering**: Order amount categories, customer age groups (T0015)
10. **Config-Driven Rules**: YAML-based cleaning and transformation rules (T0012, T0017)

## Project Structure

```
Airflow/
├── dags/
│   └── amazon_etl_dag.py        # Main Amazon ETL DAG
│   └── master_orchestrator_dag.py  # Master DAG (T0023)
│   └── amazon_reporting_dag.py     # Reporting with dependencies (T0025)
├── scripts/
│   ├── Extract.py               # Data extraction (T0007)
│   ├── TransformAmazon.py       # Amazon transformations (T0008-T0017)
│   ├── Load.py                  # Multi-target loading (T0007, T0018-T0022)
│   ├── cleaning_utils.py        # Reusable cleaning utilities (T0008, T0011)
│   └── config_loader.py         # YAML/JSON config loader (T0012)
├── data/
│   ├── raw/
│   │   └── amazon.csv           # Source Amazon orders
│   ├── processed/
│   │   ├── amazon_cleaned_data.csv
│   │   ├── orders_summary.csv
│   │   └── rejected_records.csv # Failed records (T0022)
│   └── staging/                 # Temporary files
├── config/
│   ├── amazon_cleaning_rules.yaml    # Cleaning config (T0012)
│   └── amazon_etl_config.yaml        # Transform config (T0017)
├── data_models/
│   └── models.py                # Data model definitions
├── docs/
│   └── Implementation_Snippets.md    # Task documentation (T0007-T0027)
├── tests/
│   └── test_etl_pipeline.py     # Unit tests for utilities
├── Docker/
│   ├── docker-compose.yaml      # Airflow services
│   ├── DOCKER_SETUP.md         # Docker setup guide
│   └── start_airflow.ps1       # Windows startup script
└── logs/                        # Airflow task logs
```

## Prerequisites

- Docker Desktop
- Python 3.8+
- Git

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/SammyBoy-09/Airflow_Amazon.git
cd Airflow_Amazon
```

### 2. Start Airflow Services

```bash
cd Docker
docker-compose up -d
```

This will start:
- **PostgreSQL**: Database for Airflow metadata and processed data
- **Redis**: Message broker for task queuing
- **Airflow Webserver**: UI accessible at http://localhost:8080
- **Airflow Scheduler**: DAG scheduling and execution

### 3. Access Airflow UI

Open your browser and navigate to: http://localhost:8080

Default credentials (configure in `.env`):
- Username: `airflow`
- Password: `airflow`

### 4. Place Source Data

Put your Amazon order data file in:
```
data/raw/amazon.csv
```

## Usage

### Running the ETL Pipeline

#### Manual Trigger
1. Go to http://localhost:8080
2. Find the `amazon_etl` DAG
3. Click the play button to trigger manually

#### CLI Trigger
```bash
docker exec docker-webserver-1 airflow dags trigger amazon_etl
```

#### Scheduled Runs
The DAG runs automatically on a daily schedule at midnight (configurable).

### Configuration Options

#### Load Type (T0019: Full vs Incremental)
In `dags/amazon_etl_dag.py`, adjust the load mode:

```python
load_type="full"        # T0019: Replaces entire table
# OR
load_type="incremental" # T0019: Appends new records
```

#### Bulk Load Chunk Size (T0018)
Adjust the batch size for better performance:

```python
bulk_chunk_size=1000  # Default: 1000 rows per batch
bulk_chunk_size=500   # Smaller batches for memory-constrained systems
bulk_chunk_size=5000  # Larger batches for faster loading
```

#### Upsert Mode (T0021)
Enable upsert logic to handle constraint violations:

```python
upsert_key="CustomerID"  # T0021: Enable upsert with primary key
# OR
upsert_key=None          # Disable upsert, use standard bulk load
```

### Checking Task Status

```bash
docker exec docker-webserver-1 airflow tasks states-for-dag-run amazon_etl <run_id>
```

### Viewing Logs

Logs are stored in:
```
logs/dag_id=amazon_etl/
```

Or view in the Airflow UI under each task.

## Output Files

After successful execution:

- **CSV**: `data/processed/amazon_cleaned_data.csv`
- **Excel**: `data/processed/amazon_cleaned_data.xlsx` (optional)
- **PostgreSQL Table**: `customers_cleaned`
- **Order Summary**: `data/processed/orders_summary.csv`
- **Rejected Records**: `data/processed/rejected_records.csv` (T0022)
- **Reject Table**: PostgreSQL `rejected_records` table with error context (T0022)

## Database Connection

The pipeline connects to PostgreSQL:

```
Host: postgres (Docker network)
Port: 5432
Database: airflow
Username: airflow
Password: airflow
```

Access via pgAdmin or CLI:
```bash
docker exec -it docker-postgres-1 psql -U airflow -d airflow
```

Query the data:
```sql
SELECT * FROM customers_cleaned LIMIT 10;
```

## Implemented Tasks (T0007-T0022)

### Extract & Load (T0007)
- CSV/Excel file reading with format auto-detection
- Multi-target output (CSV, Excel, PostgreSQL)

### Transform Pipeline (T0008-T0017)
- **T0008**: Reusable cleaning utilities (`cleaning_utils.py`)
- **T0009**: Data type handling and phone standardization
- **T0010**: Duplicate removal by OrderID
- **T0011**: Missing data strategies (mean/median/drop)
- **T0012**: YAML-driven cleaning rules
- **T0013**: Aggregations (groupBy, sum, min, max)
- **T0014**: Min-max normalization
- **T0015**: Feature engineering (categories, age groups)
- **T0016**: Date/time transformations
- **T0017**: Config-based transformation rules

### Load Pipeline (T0018-T0022)
- **T0018**: Bulk load with configurable chunk size
- **T0019**: Incremental vs full load modes
- **T0020**: Constraint violation handling
- **T0021**: Upsert logic with primary key matching
- **T0022**: Reject table with error logging

See [docs/Implementation_Snippets.md](docs/Implementation_Snippets.md) for detailed code examples.

### Orchestration (T0023-T0027)
- ✅ **T0023**: Master orchestrator DAG (`master_orchestrator_dag.py`)
- ✅ **T0024**: Event-driven triggering with FileSensor
- ✅ **T0025**: Multi-DAG dependency management with ExternalTaskSensor
- ✅ **T0026**: Backfill & catchup features for historical data processing
- ✅ **T0027**: Comprehensive failure handling (retries, exponential backoff, callbacks)

## DAG Architecture

The project contains **3 DAGs** that work together to provide end-to-end data processing and reporting:

### 1. `amazon_etl` - Main ETL Pipeline
**File**: `dags/amazon_etl_dag.py`  
**Schedule**: Triggered by `master_orchestrator` (no auto-schedule)  
**Purpose**: Core data processing pipeline for Amazon order data

**Task Flow**:
```
wait_for_amazon_file (FileSensor) → extract_amazon_data → transform_amazon_data → load_amazon_data
```

**What each task does**:
- **wait_for_amazon_file**: Monitors `data/raw/amazon.csv`, checks every 30s (T0024: Event-driven)
- **extract_amazon_data**: Reads CSV/Excel, returns DataFrame via XCom
- **transform_amazon_data**: Applies 10+ operations (trim, dedupe, type fixes, aggregations, feature engineering - T0008-T0017)
- **load_amazon_data**: Saves to CSV, Excel, PostgreSQL with bulk loading and error handling (T0018-T0022)

**Features**:
- Event-driven triggering (waits for file arrival)
- 3 retries with exponential backoff (5min → 10min → 20min)
- Custom failure callback for detailed error logging
- Catchup enabled for historical data processing
- Reject table for failed records

---

### 2. `master_orchestrator` - Coordination Layer
**File**: `dags/master_orchestrator_dag.py`  
**Schedule**: `@daily` (runs at midnight automatically)  
**Purpose**: Centralized control to trigger all ETL and reporting pipelines in sequence

**Task Flow**:
```
start_orchestration → trigger_amazon_etl → trigger_amazon_reporting → orchestration_complete
```

**What each task does**:
- **start_orchestration**: Logs orchestration start
- **trigger_amazon_etl**: Triggers `amazon_etl` DAG, waits for completion (T0023)
- **trigger_amazon_reporting**: Triggers `amazon_reporting` DAG, waits for completion (T0023)
- **orchestration_complete**: Logs completion summary

**Features**:
- Single entry point for entire pipeline
- Sequential execution (ETL → Reporting)
- Passes execution context to child DAGs
- Can be extended for parallel execution of multiple pipelines

**How to use**: Just trigger this DAG (manually or let it run on schedule) - it handles everything else.

---

### 3. `amazon_reporting` - Daily Reporting Pipeline
**File**: `dags/amazon_reporting_dag.py`  
**Schedule**: Triggered by `master_orchestrator` (no auto-schedule)  
**Purpose**: Generate daily reports after ETL completes successfully

**Task Flow**:
```
wait_for_amazon_etl_completion (ExternalTaskSensor) → generate_daily_report → send_report_notification
```

**What each task does**:
- **wait_for_amazon_etl_completion**: Waits for `amazon_etl.load_amazon_data` to succeed (T0025: Cross-DAG dependency)
- **generate_daily_report**: 
  - Reads cleaned data from `amazon_cleaned_data.csv`
  - Calculates metrics (total orders, revenue, avg order value, unique customers)
  - Saves to `orders_summary.xlsx` (new sheet per day)
  - Appends to `daily_reports.csv` and `daily_reports.xlsx` with timestamps
- **send_report_notification**: Logs completion (placeholder for email/Slack)

**Features**:
- Cross-DAG dependency via ExternalTaskSensor (T0025)
- Multi-format output (Excel sheets, CSV, consolidated Excel)
- Time-stamped reports for trend analysis
- Safe failure handling (won't process incomplete data)

---

## Execution Flow

### Complete Pipeline Flow (Master-Driven):

```
USER TRIGGERS: master_orchestrator (or runs automatically at midnight)
    ↓
[Master DAG starts]
    ↓
Triggers amazon_etl
    ↓
[amazon_etl: wait_for_file → extract → transform → load]
    ↓ (ETL completes successfully)
    ↓
Triggers amazon_reporting
    ↓
[amazon_reporting: wait_for_etl → generate_report → notify]
    ↓ (Reporting completes)
    ↓
[Master DAG completes: All pipelines successful]
```

**Timeline Example**:
```
00:00:00 - master_orchestrator: Scheduled run starts
00:00:01 - master_orchestrator: Triggers amazon_etl
00:00:02 - amazon_etl: FileSensor starts checking for file
00:05:00 - amazon_etl: File detected, Extract starts
00:07:30 - amazon_etl: Transform completes
00:10:15 - amazon_etl: Load completes (SUCCESS)
00:10:16 - master_orchestrator: Triggers amazon_reporting
00:10:17 - amazon_reporting: ExternalTaskSensor detects ETL success
00:10:18 - amazon_reporting: Generating report
00:11:00 - amazon_reporting: Report saved (SUCCESS)
00:11:01 - master_orchestrator: All pipelines complete
```

**Output Files After Full Run**:
- `data/processed/amazon_cleaned_data.csv` (cleaned ETL output)
- `data/processed/amazon_cleaned_data.xlsx` (cleaned ETL output)
- `data/processed/orders_summary.csv` (aggregated stats from transform)
- `data/processed/orders_summary.xlsx` (multiple sheets: one per day)
- `data/processed/daily_reports.csv` (all reports with timestamps)
- `data/processed/daily_reports.xlsx` (all reports in single sheet)
- `data/processed/rejected_records.csv` (failed records if any)
- PostgreSQL: `customers_cleaned` and `rejected_records` tables

**Features**: Event-driven triggering, 3 retries with exponential backoff, catchup enabled, custom failure logging

### Master Orchestrator (`master_orchestrator`)
Coordinates execution of multiple ETL pipelines from a single entry point.
- Triggers child DAGs sequentially or in parallel
- Passes execution context to child DAGs
- Centralized logging and monitoring

### Reporting Pipeline (`amazon_reporting`)
Generates daily reports with cross-DAG dependency:
- Waits for `amazon_etl` to complete successfully (T0025)
- Reads cleaned data and generates statistics
- Sends notifications (placeholder for email/Slack)

## Future Enhancements

### Smart Incremental Loading (Option 2)
Currently documented in `scripts/Load.py` as TODO:

- Metadata table for tracking last load timestamps
- Date-based filtering for new/updated records only
- Automatic first-run detection
- Estimated effort: ~30-35 lines across 3 files

### Potential Extensions
- Email/Slack notifications in failure callback (T0027)
- S3 file sensor for cloud-based triggering
- Advanced monitoring dashboards
- Data quality metrics tracking

## Troubleshooting

### Containers Not Starting
```bash
docker-compose down
docker-compose up -d
```

### DAG Not Appearing
- Check for syntax errors in `dags/amazon_etl_dag.py`
- Verify the DAG is in the `/opt/airflow/dags` folder
- Wait 30 seconds for scheduler to parse DAGs

### Task Failures
- Check logs in Airflow UI
- Verify source data file exists: `data/raw/amazon.csv`
- Check PostgreSQL connection
- Review rejected records in `data/processed/rejected_records.csv` (T0022)

### Permission Issues
```bash
sudo chown -R $(id -u):$(id -g) .
```

## Technologies Used

- **Apache Airflow 2.8.3**: Workflow orchestration
- **PostgreSQL 15**: Data storage
- **Redis 7**: Task queue
- **Pandas**: Data manipulation
- **SQLAlchemy**: Database ORM
- **Docker**: Containerization
- **Python 3.8**: Core language

