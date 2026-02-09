# Weather Prediction Pipeline for Edinburgh, Scotland

A real-time streaming data pipeline that predicts weather conditions using Apache Kafka, Flink, MongoDB Atlas, Trino, and Neural Prophet for time-series forecasting.

## Overview

Edinburgh's unpredictable weather makes planning outdoor activities challenging, especially for content creators who love making reels and TikTok videos. I tried to make things easier for them. This project implements a machine learning-powered weather forecasting system that ingests real-time weather data, processes it through a streaming pipeline, and generates next-hour predictions using Neural Prophet.

## Architecture

The system follows a modern data streaming architecture with the following data flow:

```
┌─────────────────┐
│  Open-Meteo API │  (Weather Data Source)
└────────┬────────┘
         │
         │ HTTP Requests every 30s
         ▼
┌─────────────────┐
│ Python Producer │  (Data Ingestion)
└────────┬────────┘
         │
         │ Publishes Messages
         ▼
┌─────────────────┐
│  Apache Kafka   │  (Message Streaming)
│  (KRaft Mode)   │
└────────┬────────┘
         │
         │ Consumes Stream
         ▼
┌─────────────────┐
│  Apache Flink   │  (Stream Processing)
│ (Scala/Java)    │
└────────┬────────┘
         │
         │ Writes Processed Data
         ▼
┌─────────────────┐
│ MongoDB Atlas   │  (Data Persistence)
└────────┬────────┘
         │
         │ SQL Queries
         ▼
┌─────────────────┐
│     Trino       │  (Distributed Query Engine)
└────────┬────────┘
         │
         │ Export to CSV
         ▼
┌─────────────────┐
│ Neural Prophet  │  (ML Time Series Forecasting)
└────────┬────────┘
         │
         ▼
   Weather Predictions
```

## Technology Stack

**Data Ingestion & Streaming:**
- Apache Kafka (KRaft mode) - Distributed message streaming platform
- Python Producer - Fetches data from Open-Meteo API

**Stream Processing:**
- Apache Flink (Scala) - Real-time stream processing framework

**Data Storage:**
- MongoDB Atlas - Cloud-native NoSQL database for weather readings

**Query & Analytics:**
- Trino - Distributed SQL query engine for data aggregation

**Machine Learning:**
- Neural Prophet - Facebook's time-series forecasting library based on PyTorch

## Web Interfaces

Once the pipeline is running, you can access these monitoring interfaces:

- **Kafka UI (Provectus):** http://localhost:8080 - Monitor Kafka topics, messages, and consumer groups
- **Flink Dashboard:** http://localhost:9000 - View running jobs, task managers, and job metrics
- **Trino UI:** http://localhost:9100 - Query interface and cluster information

## Prerequisites

### Required Software

- **Docker Desktop** 20.10 or higher (Windows/Mac) or Docker Engine (Linux)
- **WSL2** (Windows users only) - Required for Docker Desktop on Windows
- **Python** 3.12 or 3.13
- **Java** 11 or later (for Flink)
- **Scala** 2.12
- **SBT** 1.x (Scala Build Tool)

### External Services

You will need a MongoDB Atlas account (the free tier is sufficient):

1. Create an account at [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
2. Create a cluster and obtain your connection string
3. Whitelist your IP address in Network Access settings
4. Create a database user with read/write permissions

### System Requirements

- Minimum 8GB RAM (recommended 16GB)
- 10GB free disk space
- Linux, macOS, or Windows with WSL2

## Installation & Setup

### Step 1: Initial Environment Setup

**For Windows Users:**

First, ensure WSL2 is installed and running. Open PowerShell as Administrator and verify WSL2:

```powershell
wsl --status
```

If WSL2 is not installed, run:

```powershell
wsl --install
```

Then start Docker Desktop from the Start menu. Docker Desktop will automatically use WSL2 as its backend.

**For macOS/Linux Users:**

Start Docker Desktop (macOS) or ensure Docker service is running (Linux):

```bash
# macOS - Launch Docker Desktop from Applications
# Linux
sudo systemctl start docker
```

**Verify Docker is Running:**

Open a terminal (or Command Prompt as Administrator on Windows) and confirm Docker is operational:

```bash
docker info
```

You should see output showing Docker's system information. If you get a connection error, ensure Docker Desktop is fully started (this can take 30-60 seconds after launching).

### Step 2: Clone the Repository

```bash
git clone https://github.com/your-username/Reels_in_Scotland.git
cd Reels_in_Scotland
```

### Step 3: Start Infrastructure Services

Navigate to your project directory and launch Kafka in KRaft mode using Docker Compose:

```bash
docker-compose -f Docker-ComposeISR.yml up -d
```

Wait approximately 30-45 seconds for all services to initialize properly. You can verify the services are running:

```bash
docker-compose -f Docker-ComposeISR.yml ps
```

All containers should show status "Up". At this point, you can access the Kafka UI at http://localhost:8080 to confirm Kafka is ready.

### Step 4: Configure MongoDB Connection

Create the Trino MongoDB catalog configuration:

```bash
# Linux/macOS
cp query_trino/catalog/mongo.properties.example query_trino/catalog/mongo.properties

# Windows (PowerShell)
Copy-Item query_trino\catalog\mongo.properties.example query_trino\catalog\mongo.properties
```

Edit `query_trino/catalog/mongo.properties` with your MongoDB Atlas credentials:

```properties
connector.name=mongodb
mongodb.connection-uri=mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@your-cluster.mongodb.net/weather_db
```

Replace `YOUR_USERNAME`, `YOUR_PASSWORD`, and `your-cluster` with your actual MongoDB Atlas credentials.

### Step 5: Start the Weather Data Producer

Open your first terminal window and navigate to the producer directory.

**For Windows (PowerShell):**

```powershell
cd producer
python -m venv venv

# Set execution policy for the current session to allow script execution
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Activate the virtual environment
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Start the producer
python main.py
```

**For Linux/macOS:**

```bash
cd producer
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

Leave this process running. It will fetch and publish weather data every 30 seconds. You should see log messages indicating successful data fetching and publishing to Kafka. Verify data is flowing by checking the Kafka UI at http://localhost:8080 - you should see your weather topic receiving messages.

### Step 6: Build and Run the Flink Consumer

Open a second terminal window and navigate to the Flink consumer directory.

**Set MongoDB Connection String:**

This environment variable allows the Flink application to connect to your MongoDB Atlas cluster.

For Windows (PowerShell):
```powershell
cd flink_consumer
$env:MONGO_CONNECTION_STRING="mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@your-cluster.mongodb.net/weather_db"
```

For Linux/macOS:
```bash
cd flink_consumer
export MONGO_CONNECTION_STRING="mongodb+srv://YOUR_USERNAME:YOUR_PASSWORD@your-cluster.mongodb.net/weather_db"
```

**Build and Run the Flink Application:**

```bash
# Clean previous builds
sbt clean

# Compile the Scala code
sbt compile
```

The compilation process may take 2-3 minutes the first time as SBT downloads all dependencies. Once compilation succeeds, start the Flink job:

```bash
sbt run
```

The Flink job will start processing the Kafka stream and writing data to MongoDB. You can monitor the job's progress at the Flink Dashboard: http://localhost:9000. You should see one running job with metrics showing records processed.

### Step 7: Data Collection Period

**Important:** Allow the pipeline to run for at least 2-3 hours to accumulate sufficient historical data for meaningful predictions. During this time:

- Keep both the producer (Step 5) and Flink consumer (Step 6) running
- Monitor data arrival in your MongoDB Atlas dashboard under the Collections tab
- Check the Flink dashboard to ensure continuous processing
- Verify no errors appear in either terminal

For best forecasting results, collecting 6-12 hours of continuous data is recommended. You can leave the pipeline running overnight.

### Step 8: Start Trino and Query Historical Data

After collecting sufficient data, you're ready to query and export the aggregated weather readings.

**Start Trino:**

Open a third terminal window:

```bash
cd query_trino
docker-compose -f Docker-ComposeTrino.yml up -d
```

Wait about 30 seconds for Trino to fully start. You can access the Trino web interface at http://localhost:9100 to confirm it's running.

**Connect to MongoDB via Trino CLI:**

Enter the Trino interactive shell:

```bash
docker exec -it trino trino
```

You should see the Trino prompt: `trino>`

**Initial Database Setup and Exploration:**

First, verify that Trino can see your MongoDB catalog:

```sql
SHOW CATALOGS;
```

You should see `mongo` in the list. Now connect to your MongoDB database:

```sql
USE mongo.weather_pipeline;
```

Note that `weather_pipeline` should match your actual database name in MongoDB Atlas. If you used a different database name, adjust accordingly.

Show available tables (collections):

```sql
SHOW TABLES;
```

You should see your collections, such as `aggregated_2min` or `weather_readings`.

Describe the table structure to understand the schema:

```sql
DESCRIBE aggregated_2min;
```

**Run Aggregation Queries:**

Now you can run analytical queries on your weather data:

```sql
SELECT 
    city,
    AVG(CAST(temperature AS DOUBLE)) as avg_temp,
    AVG(CAST(precipitation AS DOUBLE)) as avg_precip,
    COUNT(*) as record_count
FROM mongo.weather_pipeline.aggregated_2min
GROUP BY city;
```

Explore recent data to understand the time range available:

```sql
SELECT 
    avg_timestamp,
    temperature,
    precipitation
FROM mongo.weather_pipeline.aggregated_2min
ORDER BY avg_timestamp DESC
LIMIT 10;
```

**Export Query Results to CSV:**

Once you've identified the data you need, exit the Trino CLI (type `quit;` or press Ctrl+D) and export the data directly from your terminal.

The following command queries MongoDB via Trino and exports the results to a CSV file with headers. This example exports data from a specific time window:

```bash
docker exec -i trino trino \
  --catalog mongo \
  --schema weather_pipeline \
  --output-format CSV_HEADER \
  --execute "SELECT * FROM mongo.weather_pipeline.aggregated_2min WHERE CAST(REPLACE(avg_timestamp, 'T', ' ') AS TIMESTAMP) >= TIMESTAMP '2025-01-23 22:15:00' AND CAST(REPLACE(avg_timestamp, 'T', ' ') AS TIMESTAMP) <= TIMESTAMP '2025-01-23 22:30:00';" \
  > weather_export_sample.csv
```

**Important notes about this command:**

- Replace the timestamp values with your actual data range
- The `CAST(REPLACE(avg_timestamp, 'T', ' ') AS TIMESTAMP)` converts MongoDB's ISO 8601 format timestamps to SQL timestamps
- The `--output-format CSV_HEADER` includes column names in the first row
- The `>` redirects output to a file named `weather_export_sample.csv`

For exporting all available data without time filters:

```bash
docker exec -i trino trino \
  --catalog mongo \
  --schema weather_pipeline \
  --output-format CSV_HEADER \
  --execute "SELECT * FROM mongo.weather_pipeline.aggregated_2min;" \
  > weather_export_all.csv
```

The CSV file will be created in your current directory and is now ready for machine learning model training.

### Step 9: Train the Model and Generate Predictions

In a new terminal window, navigate to the machine learning directory.

**For Windows (PowerShell):**

```powershell
cd ml_prophet
python -m venv venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py
```

**For Linux/macOS:**

```bash
cd ml_prophet
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python main.py
```

The Neural Prophet model will:

1. Load the exported CSV data
2. Prepare the time series for training
3. Train the forecasting model on historical patterns
4. Generate next-hour weather predictions with confidence intervals

Training time depends on the amount of data collected (typically 2-5 minutes for several hours of data).

## Sample Output

After training completes, you'll see predictions like:

```
Predicted Weather for Next Hour:
Temperature: 8.5°C
Precipitation: 0.2mm
Wind Speed: 12.3 km/h
Confidence: 87%
```

## Project Structure

```
Reels_in_Scotland/
├── producer/                   # Python weather data ingestion service
│   ├── main.py                 # Producer entry point
│   ├── config.py               # API and Kafka configuration
│   └── requirements.txt        # Python dependencies
│
├── flink_consumer/             # Scala Flink stream processor
│   ├── build.sbt               # SBT build configuration
│   ├── src/                    # Scala source files
│   └── project/                # SBT project settings
│
├── query_trino/                # Trino distributed query setup
│   ├── catalog/                # Data source catalogs
│   │   └── mongo.properties    # MongoDB connection config
│   └── Docker-ComposeTrino.yml # Trino container setup
│
├── ml_prophet/                 # Neural Prophet ML forecasting
│   ├── main.py                 # Model training and prediction
│   └── requirements.txt        # Python ML dependencies
│
└── Docker-ComposeISR.yml       # Infrastructure services (Kafka)
```

## Configuration Files

**Producer Configuration** (`producer/config.py`):
- Open-Meteo API endpoints for Edinburgh weather data
- Kafka broker addresses and topic names
- Data fetch intervals (default: 30 seconds)

**Flink Configuration** (`flink_consumer/build.sbt`):
- Flink version and dependencies
- Kafka connector settings
- MongoDB sink configurations with connection details

**Trino Configuration** (`query_trino/catalog/mongo.properties`):
- MongoDB Atlas connection string
- Database and collection mappings for SQL queries

## Troubleshooting

### Windows-Specific Issues

**PowerShell Execution Policy Error:**

If you see "cannot be loaded because running scripts is disabled", run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

This temporarily allows script execution for your current PowerShell session without changing system-wide security settings.

**WSL2 Not Running:**

If Docker Desktop shows "WSL2 is not running", open PowerShell as Administrator:

```powershell
wsl --set-default-version 2
wsl --install
```

Then restart Docker Desktop.

### Kafka Connection Issues

If you see "Connection refused" errors when the producer tries to publish messages:

```bash
# Verify Kafka containers are running
docker-compose -f Docker-ComposeISR.yml ps

# Check Kafka logs for startup errors
docker-compose -f Docker-ComposeISR.yml logs -f kafka

# Restart Kafka if needed
docker-compose -f Docker-ComposeISR.yml restart
```

Wait 30-45 seconds after restart before running the producer again.

### MongoDB Connection Timeout

Common causes and solutions:

- **IP Whitelist:** Verify your current IP address is whitelisted in MongoDB Atlas Network Access settings. If you're on a dynamic IP, you may need to whitelist `0.0.0.0/0` (all IPs) for development, though this is less secure.
- **Connection String Format:** Ensure the format is `mongodb+srv://username:password@cluster.mongodb.net/database_name`
- **Special Characters in Password:** If your password contains special characters like `@`, `#`, or `/`, they must be URL-encoded (e.g., `@` becomes `%40`)
- **Firewall/VPN:** Temporarily disable VPN or check firewall rules that might block MongoDB Atlas connections (port 27017)
- **Wrong Credentials:** Double-check username and password in MongoDB Atlas Database Access

Test your connection string using the MongoDB shell before using it in the application.

### Flink Out of Memory Errors

If Flink crashes with `OutOfMemoryError: Java heap space`:

**Docker Desktop (Windows/Mac):**
1. Open Docker Desktop
2. Go to Settings → Resources → Advanced
3. Increase Memory to at least 6GB (8GB recommended)
4. Click "Apply & Restart"

**Linux Docker Engine:**

Edit `/etc/docker/daemon.json`:

```json
{
  "default-address-pools": [
    {
      "base": "172.17.0.0/12",
      "size": 24
    }
  ],
  "default-ulimits": {
    "memlock": {
      "Hard": -1,
      "Soft": -1
    }
  }
}
```

Then restart Docker:

```bash
sudo systemctl restart docker
```

### Trino Cannot Connect to MongoDB

If you see `Unable to connect to MongoDB` in Trino:

1. Verify the `mongo.properties` file has the correct connection string
2. Ensure the database name in your queries matches the actual database in MongoDB Atlas
3. Restart Trino after modifying catalog configuration:

```bash
docker-compose -f Docker-ComposeTrino.yml restart
```

4. Check Trino logs:

```bash
docker-compose -f Docker-ComposeTrino.yml logs -f
```

### Insufficient Training Data

If Neural Prophet predictions seem inaccurate or you get warnings about insufficient data:

- Ensure you have collected at least 2-3 hours of continuous data (check MongoDB Atlas Collections for record count)
- More data (6-12 hours) will produce significantly better forecasting results
- Verify data is consistently being written by checking timestamps in MongoDB - gaps indicate the producer or Flink consumer stopped

Check data continuity with this Trino query:

```sql
SELECT 
    COUNT(*) as total_records,
    MIN(avg_timestamp) as earliest_record,
    MAX(avg_timestamp) as latest_record
FROM mongo.weather_pipeline.aggregated_2min;
```

### Port Conflicts

If you see "port already in use" errors:

- Port 8080 (Kafka UI): Check if another service is using this port with `netstat -an | grep 8080` (Linux/Mac) or `netstat -an | findstr 8080` (Windows)
- Port 9000 (Flink): Ensure no other Flink instance is running
- Port 9100 (Trino): Check for other database tools using this port

Stop conflicting services or modify the port mappings in the respective Docker Compose files.

## Performance Optimization Tips

For better pipeline performance:

1. **Increase Kafka Partitions:** Edit `Docker-ComposeISR.yml` to add more partitions for parallel processing
2. **Tune Flink Parallelism:** Modify Flink configuration in `build.sbt` to match your CPU core count
3. **MongoDB Index Creation:** Create indexes on `avg_timestamp` field in MongoDB Atlas for faster queries
4. **Batch Size Tuning:** Adjust the producer's batch size in `config.py` for higher throughput

## Acknowledgments

- Weather data provided by [Open-Meteo API](https://open-meteo.com/)
- Neural Prophet developed by Facebook's Core Data Science team
- Apache Kafka, Flink, and Trino communities for excellent documentation
