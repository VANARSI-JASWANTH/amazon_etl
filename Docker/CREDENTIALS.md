# Airflow Login Credentials

## ✅ Available User Accounts

### Option 1: New Default User (Recommended)
- **Username:** `airflow`
- **Password:** `airflow`
- **Role:** Admin
- **Email:** airflow@example.com

### Option 2: Original User
- **Username:** `admin`
- **Password:** (your existing password)
- **Role:** Admin
- **Email:** admin@example.com

## 🌐 Access Airflow UI

**URL:** http://localhost:8080

**Login Steps:**
1. Open http://localhost:8080 in your browser
2. Use either set of credentials above
3. You should see the DAGs page with `customer_etl` DAG

## 🔄 Managing Users

### List All Users
```powershell
cd Docker
docker-compose exec webserver airflow users list
```

### Create New User
```powershell
docker-compose exec webserver airflow users create `
  --username <username> `
  --password <password> `
  --firstname <First> `
  --lastname <Last> `
  --role Admin `
  --email <email@example.com>
```

### Reset User Password
```powershell
docker-compose exec webserver airflow users reset-password `
  --username <username>
```

### Delete User
```powershell
docker-compose exec webserver airflow users delete `
  --username <username>
```

## 🎯 Current Status

✅ Airflow is running on http://localhost:8080
✅ User `airflow` created with password `airflow`
✅ DAG `customer_etl` is visible and active
✅ PostgreSQL database is running
✅ All services are healthy

## 🚀 Next Steps

1. **Login to Airflow UI:**
   - Go to http://localhost:8080
   - Username: `airflow`
   - Password: `airflow`

2. **Trigger the ETL DAG:**
   - Find `customer_etl` in the DAGs list
   - Make sure it's toggled ON (the toggle should be blue)
   - Click the "Play" button (▶) to trigger manually
   - Watch it run in real-time!

3. **View Results:**
   - Check processed data: `data/processed/cleaned_data.csv`
   - Or query PostgreSQL:
     ```powershell
     docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM customers_cleaned LIMIT 5;"
     ```

## 📊 Monitoring

### Check Service Status
```powershell
docker-compose ps
```

### View Logs
```powershell
# Webserver logs
docker-compose logs -f webserver

# Scheduler logs
docker-compose logs -f scheduler

# All logs
docker-compose logs -f
```

### Restart Services
```powershell
# Restart specific service
docker-compose restart webserver
docker-compose restart scheduler

# Restart all
docker-compose restart
```

## 🛑 Stop/Start Services

### Stop All Services
```powershell
docker-compose down
```

### Start All Services
```powershell
docker-compose up -d
```

### Full Reset (Clean Start)
```powershell
# WARNING: This deletes all data including users
docker-compose down -v
docker-compose up airflow-init
docker-compose up -d
```

## 🔐 Security Notes

**For Production:**
- Change default passwords immediately
- Use strong, unique passwords
- Enable HTTPS
- Configure proper authentication (LDAP, OAuth, etc.)
- Restrict network access
- Use secrets management (e.g., AWS Secrets Manager, HashiCorp Vault)

**For Development (Current Setup):**
- Default credentials are fine for local testing
- Database credentials in `.env` file
- All services running locally only

## 💡 Troubleshooting

### Can't Login?
1. Check user exists: `docker-compose exec webserver airflow users list`
2. Try the `admin` user with your existing password
3. Reset password if needed:
   ```powershell
   docker-compose exec webserver airflow users reset-password --username airflow
   ```

### Services Not Starting?
```powershell
# Check logs
docker-compose logs webserver
docker-compose logs scheduler

# Restart services
docker-compose restart
```

### Port Already in Use?
Edit `docker-compose.yaml` and change:
```yaml
ports:
  - "8081:8080"  # Change 8080 to 8081
```

Then access via http://localhost:8081

## 📚 Additional Resources

- Docker Setup Guide: `Docker/DOCKER_SETUP.md`
- Project README: `../README.md`
- Quick Start: `../QUICKSTART.md`
- Implementation Summary: `../IMPLEMENTATION_SUMMARY.md`

---

**You're all set! Login with `airflow/airflow` and start using your ETL pipeline! 🎉**
