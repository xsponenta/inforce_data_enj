# User Data ETL Project

## Overview
This project performs an ETL (Extract, Transform, Load) process to generate fake user data and load it into a PostgreSQL database using Docker.

## Prerequisites
- Docker
- Docker Compose

## Project Structure
- `inforce_data_enj.py`: Main Python ETL script
- `Dockerfile`: Docker configuration for the Python application
- `docker-compose.yml`: Docker Compose configuration
- `requirements.txt`: Python package dependencies

## Setup and Running

### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-directory>
```

### 2. Build and Run
```bash
docker-compose up --build
```

This command will:
- Build the Python ETL application Docker image
- Start a PostgreSQL database container
- Run the ETL script

### Configuration

#### Environment Variables
You can customize the following environment variables in `docker-compose.yml`:
- `POSTGRES_DB`: Database name (default: transformed_data_db)
- `POSTGRES_USER`: Database username (default: postgres)
- `POSTGRES_PASSWORD`: Database password (default: postgres)
- `POSTGRES_HOST`: Database host (default: postgres)
- `POSTGRES_PORT`: Database port (default: 5432)

### Verifying Results
After running the containers:
1. The script generates a CSV with 1000 fake user records
2. Transforms the data, validating emails and extracting domains
3. Loads the data into a PostgreSQL database

You can verify the data by connecting to the PostgreSQL database:
```bash
docker-compose exec postgres psql -U postgres -d transformed_data_db
```

Then run:
```sql
SELECT * FROM users LIMIT 10;
```

## Assumptions
- Generates 1000 fake user records
- Validates email format using a regex pattern
- Extracts email domain
- Converts signup dates to a standard format

## Notes
- Fake data is regenerated each time the script runs
- The database is truncated before inserting new data to prevent duplicates
