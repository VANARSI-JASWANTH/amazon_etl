# Docker Setup Guide for Airflow ETL Pipeline

## Prerequisites

1. **Docker Desktop** installed and running
   - Download from: https://www.docker.com/products/docker-desktop
   - Ensure Docker is running before proceeding

2. **Windows PowerShell** or Command Prompt

3. **At least 4GB RAM** allocated to Docker

## Quick Start

### Step 1: Navigate to Docker Directory
```powershell
cd Docker
```

### Step 2: Initialize Airflow (First Time Only)
```powershell
# On Windows PowerShell
docker-compose up airflow-init

# Or on Windows CMD
docker compose up airflow-init
```

This will:
- Create necessary directories
- Initialize the Airflow database
- Create the admin user

### Step 3: Start All Services
```powershell
docker-compose up -d
```

This starts:
- PostgreSQL database
- Redis (for Celery, if needed)
- Airflow Webserver (UI)
- Airflow Scheduler

### Step 4: Access Airflow UI
Open your browser and go to: **http://localhost:8080**

**Default Credentials:**
- Username: `airflow`
- Password: `airflow`

### Step 5: Verify Services are Running
```powershell
# Check running containers
docker-compose ps

# Check logs
docker-compose logs -f webserver
docker-compose logs -f scheduler
```

## Common Commands

### Start Services
```powershell
docker-compose up -d
```

### Stop Services
```powershell
docker-compose down
```

### Stop and Remove All Data (Clean Start)
```powershell
docker-compose down -v
```

### View Logs
```powershell
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f webserver
docker-compose logs -f scheduler
docker-compose logs -f postgres
```

### Restart a Service
```powershell
docker-compose restart webserver
docker-compose restart scheduler
```

### Execute Commands in Container
```powershell
# Access webserver shell
docker-compose exec webserver bash

# Run Airflow CLI commands
docker-compose exec webserver airflow dags list
docker-compose exec webserver airflow dags test customer_etl 2025-01-01
```

## Project Structure in Docker

The following directories are mounted from your host machine to the Docker containers:

```
Host Machine                Docker Container
├── dags/              →    /opt/airflow/dags
├── logs/              →    /opt/airflow/logs
├── plugins/           →    /opt/airflow/plugins
├── scripts/           →    /opt/airflow/scripts
├── data_models/       →    /opt/airflow/data_models
└── data/              →    /opt/airflow/data
    ├── raw/           →    /opt/airflow/data/raw
    ├── staging/       →    /opt/airflow/data/staging
    └── processed/     →    /opt/airflow/data/processed
```

## Configuration

### Environment Variables (.env file)

The `.env` file contains:
- `AIRFLOW_UID` - User ID for file permissions
- `POSTGRES_USER` - PostgreSQL username
- `POSTGRES_PASSWORD` - PostgreSQL password
- `POSTGRES_DB` - Database name
- `AIRFLOW__CORE__FERNET_KEY` - Encryption key

### Installed Python Packages in Docker

The following packages are automatically installed in the Airflow containers:
- pandas
- numpy
- openpyxl (for Excel files)
- pydantic (for data validation)
- PyYAML (for config files)
- scikit-learn (for ML-based imputation)
- psycopg2 (for PostgreSQL)

## Testing the ETL Pipeline in Docker

### 1. Check DAG is Loaded
```powershell
docker-compose exec webserver airflow dags list
```

You should see `customer_etl` in the list.

### 2. Test the DAG
```powershell
# Test the entire DAG
docker-compose exec webserver airflow dags test customer_etl 2025-01-01

# Test individual tasks
docker-compose exec webserver airflow tasks test customer_etl extract_data 2025-01-01
docker-compose exec webserver airflow tasks test customer_etl transform_data 2025-01-01
docker-compose exec webserver airflow tasks test customer_etl load_data 2025-01-01
```

### 3. Trigger DAG from UI
1. Go to http://localhost:8080
2. Find `customer_etl` in the DAG list
3. Toggle the DAG to "On"
4. Click the "Play" button to trigger manually

### 4. Check Processed Data
```powershell
# View processed CSV
type ..\data\processed\cleaned_data.csv

# Or access from container
docker-compose exec webserver cat /opt/airflow/data/processed/cleaned_data.csv
```

## Troubleshooting

### Issue: Port 8080 Already in Use
**Solution:** Change the port in docker-compose.yaml:
```yaml
ports:
  - "8081:8080"  # Use 8081 instead
```

### Issue: Permission Denied Errors
**Solution:** Run the init_dirs service:
```powershell
docker-compose up init_dirs
```

### Issue: DAG Not Appearing
**Solutions:**
1. Check DAG syntax:
   ```powershell
   docker-compose exec webserver python /opt/airflow/dags/customer_etl_dag.py
   ```

2. Check scheduler logs:
   ```powershell
   docker-compose logs -f scheduler
   ```

3. Restart scheduler:
   ```powershell
   docker-compose restart scheduler
   ```

### Issue: Import Errors in DAG
**Solution:** Install missing packages:
1. Edit docker-compose.yaml
2. Add package to `_PIP_ADDITIONAL_REQUIREMENTS`
3. Restart services:
   ```powershell
   docker-compose down
   docker-compose up -d
   ```

### Issue: Database Connection Errors
**Solution:** 
1. Check PostgreSQL is running:
   ```powershell
   docker-compose ps postgres
   ```

2. Check connection string in docker-compose.yaml
3. Restart all services:
   ```powershell
   docker-compose restart
   ```

## Monitoring

### Check Container Health
```powershell
docker-compose ps
```

All containers should show "healthy" or "running".

### Check Resource Usage
```powershell
docker stats
```

### Access PostgreSQL Database
```powershell
# Connect to PostgreSQL
docker-compose exec postgres psql -U airflow -d airflow

# List tables
\dt

# Query cleaned data
SELECT * FROM customers_cleaned LIMIT 5;

# Exit
\q
```

## Best Practices

1. **Always use docker-compose from the Docker/ directory**
   ```powershell
   cd Docker
   docker-compose up -d
   ```

2. **Check logs regularly**
   ```powershell
   docker-compose logs -f scheduler
   ```

3. **Clean up old containers periodically**
   ```powershell
   docker system prune -a
   ```

4. **Back up your data**
   - Data in `data/` folder is persisted on host
   - PostgreSQL data is in Docker volume `postgres-db`

5. **Test DAGs locally before deploying**
   ```powershell
   python dags/customer_etl_dag.py
   ```

## Updating Configuration

### Add New Python Package
1. Edit `docker-compose.yaml`
2. Add to `_PIP_ADDITIONAL_REQUIREMENTS`
3. Restart:
   ```powershell
   docker-compose down
   docker-compose up -d
   ```

### Update DAG
1. Edit DAG file in `dags/` folder
2. Changes are automatically picked up (no restart needed)
3. Wait ~30 seconds for scheduler to detect changes

### Update ETL Scripts
1. Edit files in `scripts/` folder
2. Restart scheduler:
   ```powershell
   docker-compose restart scheduler
   ```

## Production Deployment

For production, consider:

1. **Use Celery Executor** for parallel task execution
2. **Set up proper authentication** (change default password)
3. **Configure external PostgreSQL** database
4. **Set up monitoring** (Prometheus, Grafana)
5. **Use secrets management** (Docker secrets, Vault)
6. **Enable HTTPS** for web server
7. **Set up backup strategy** for database

## Additional Resources

- Airflow Documentation: https://airflow.apache.org/docs/
- Docker Documentation: https://docs.docker.com/
- Project README: ../README.md
- Quick Start Guide: ../QUICKSTART.md

## Summary

Your Airflow ETL pipeline is now running in Docker! 🎉

**To start working:**
```powershell
cd Docker
docker-compose up -d
# Open http://localhost:8080
```

**To stop:**
```powershell
cd Docker
docker-compose down
```
