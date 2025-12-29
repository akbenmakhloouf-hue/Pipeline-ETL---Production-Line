# ETL Pipeline: Production Line Data Integration

## 📋 Project Overview

A complete ETL (Extract, Transform, Load) pipeline for integrating production line sensor data and quality control systems into a centralized SQLite database. This project enables manufacturing plants to monitor daily operations, track quality metrics, and identify production issues.

## 🎯 Learning Objectives Achieved

- Extract data from CSV files with proper error handling
- Clean and validate industrial sensor data
- Combine data from multiple sources
- Create and populate a normalized SQLite database
- Implement ETL patterns (incremental load, aggregation, quality checks)

## 📊 Data Sources

### Source 1: Production Sensor Data
- **Dataset**: Smart Manufacturing IoT-Cloud Monitoring Dataset
- **Link**: [Kaggle Dataset](https://www.kaggle.com/datasets/ziya07/smart-manufacturing-iot-cloud-monitoring-dataset)
- **Contains**: Temperature, pressure, vibration, power readings from machines

### Source 2: Quality Inspection Data
- **Dataset**: Industrial IoT Fault Detection Dataset
- **Link**: [Kaggle Dataset](https://www.kaggle.com/datasets/ziya07/industrial-iot-fault-detection-dataset)
- **Contains**: Quality checks, defect flags, sensor status

## 🏗️ Architecture

```
┌─────────────────┐
│  CSV Files      │
│  - Sensor Data  │
│  - Quality Data │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   EXTRACT       │
│  - Read CSVs    │
│  - Filter dates │
│  - Handle errors│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  TRANSFORM      │
│  - Clean data   │
│  - Validate     │
│  - Join sources │
│  - Aggregate    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    LOAD         │
│  - Create DB    │
│  - Load tables  │
│  - Validate     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ production.db   │
│  3 Tables:      │
│  - sensor_readings
│  - quality_checks
│  - hourly_summary
└─────────────────┘
```

## 📁 Project Structure

```
etl-pipeline/
├── etl_pipeline.py          # Main ETL pipeline script
├── test_pipeline.py         # Database validation tests
├── production.db            # SQLite database (generated)
├── sample_queries.sql       # SQL query examples (generated)
├── requirements.txt         # Python dependencies
├── README.md               # This file
└── data/
    ├── sensor_data.csv     # Input: Sensor readings
    └── quality_data.csv    # Input: Quality checks
```

## 🚀 Getting Started

### Prerequisites

```bash
Python 3.8+
pandas
numpy
sqlite3 (built-in)
tabulate (for testing)
```

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/etl-pipeline.git
cd etl-pipeline
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Download datasets from Kaggle and place them in the `data/` directory

### Running the Pipeline

1. **Execute the ETL pipeline**:
```bash
python etl_pipeline.py
```

2. **Run validation tests**:
```bash
python test_pipeline.py
```

## 🔧 Pipeline Phases

### Phase 1: EXTRACT
- Reads CSV files with robust error handling
- Filters sensor data for last 7 days
- Handles multiple text encodings (UTF-8, ISO-8859-1)
- Extracts only completed quality inspections

### Phase 2: TRANSFORM

#### Data Cleaning
- Removes error codes (-999, -1, NULL)
- Validates sensor ranges:
  - Temperature: 0-150°C
  - Pressure: 0-10 bar
  - Vibration: 0-100 mm/s
- Forward fills missing values
- Adds data quality flags (good/estimated/invalid)

#### Data Standardization
- Converts timestamps to datetime format
- Lowercases machine/sensor names
- Removes spaces from column names
- Generates unique record IDs

#### Data Integration
- Left joins sensor and quality data
- Matches on timestamp and machine ID
- Handles missing quality records
- Adds quality status indicators

#### Aggregation
- Creates hourly summaries per machine
- Calculates avg, min, max, std for sensors
- Computes defect rates
- Counts quality checks per hour

### Phase 3: LOAD

#### Database Schema

**Table 1: sensor_readings**
```sql
CREATE TABLE sensor_readings (
    record_id TEXT PRIMARY KEY,
    timestamp DATETIME,
    line_id TEXT,
    machine_id TEXT,
    temperature REAL,
    pressure REAL,
    vibration REAL,
    power REAL,
    data_quality TEXT
);
```

**Table 2: quality_checks**
```sql
CREATE TABLE quality_checks (
    check_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME,
    line_id TEXT,
    machine_id TEXT,
    result TEXT,
    defect_type TEXT
);
```

**Table 3: hourly_summary**
```sql
CREATE TABLE hourly_summary (
    summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
    hour DATETIME,
    line_id TEXT,
    machine_id TEXT,
    avg_temperature REAL,
    min_temperature REAL,
    max_temperature REAL,
    avg_pressure REAL,
    avg_vibration REAL,
    total_checks INTEGER,
    defect_count INTEGER,
    defect_rate REAL
);
```

## 📈 Sample Queries

### Total Records Loaded
```sql
SELECT COUNT(*) FROM sensor_readings;
```

### High Defect Rate Hours
```sql
SELECT hour, line_id, machine_id, defect_rate
FROM hourly_summary
WHERE defect_rate > 5.0
ORDER BY defect_rate DESC;
```

### Temperature Statistics by Machine
```sql
SELECT
    machine_id,
    AVG(temperature) as avg_temp,
    MIN(temperature) as min_temp,
    MAX(temperature) as max_temp
FROM sensor_readings
GROUP BY machine_id;
```

### Data Quality Distribution
```sql
SELECT data_quality, COUNT(*) as count
FROM sensor_readings
GROUP BY data_quality;
```

## 🧪 Testing & Validation

The `test_pipeline.py` script runs 10 validation tests:

1.  Total records in each table
2.  Hourly summary samples
3.  High defect rate detection
4.  Data quality distribution
5.  Sensor-quality join verification
6.  Temperature statistics
7.  Data time range coverage
8.  Quality check distribution
9.  Machine defect analysis
10. Database integrity checks

## 🔍 Key Features

- **Error Handling**: Graceful handling of missing files, encoding issues, invalid data
- **Data Quality Tracking**: Flags for good/estimated/invalid readings
- **Incremental Processing**: Filters for recent data (last 7 days)
- **Normalization**: Standardized formats and naming conventions
- **Aggregation**: Hourly summaries for trend analysis
- **Validation**: Comprehensive testing suite with integrity checks

## 📊 Expected Outputs

After running the pipeline successfully:

- `production.db`: SQLite database with 3 populated tables
- Console output showing extraction, transformation, and loading progress
- Validation test results with data statistics
- `sample_queries.sql`: Reference SQL queries

## 🐛 Troubleshooting

### Common Issues

**Issue**: File not found error
```
Solution: Ensure CSV files are in the correct path or update file paths in etl_pipeline.py
```

**Issue**: Encoding errors
```
Solution: The pipeline automatically tries multiple encodings (UTF-8, ISO-8859-1, Latin1)
```

**Issue**: Empty database tables
```
Solution: Check that CSV files have data and match expected column names
```

## 📝 Future Enhancements

- [ ] Add real-time streaming support
- [ ] Implement incremental loads (append new data only)
- [ ] Add data visualization dashboard
- [ ] Schedule automated pipeline runs
- [ ] Add email alerts for high defect rates
- [ ] Implement data lineage tracking
- [ ] Add support for additional data sources

## 👥 Contributors

- Benmakhlouf khaled - Student in Management and Industrial Maintenance Engineering-ENSTA-

## 🙏 Acknowledgments

- Kaggle datasets for manufacturing IoT data
- Manufacturing plant operations team for requirements
- Data engineering community for ETL best practices

---

**Last Updated**: December 2025  
**Version**: 1.0.0  
**Status**: ✅ Production Ready
