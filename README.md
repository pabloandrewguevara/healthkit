# HealthKit ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for processing Apple HealthKit data. This project extracts health and workout data from Apple Health exports, transforms it into a structured format, and loads it into BigQuery for analysis and visualization.

## Project Structure

```
healthkit/
├── src/                     # Core application code
│   ├── data_processing.py   # Health and workout data processing logic
│   └── config.py            # Configuration management
├── utils/                   # Utility modules
│   ├── bigquery_utils.py    # BigQuery data upload utilities
│   └── logging_config.py    # Logging configuration
├── scripts/                 # Standalone scripts
│   └── refresh.py           # Main script for data processing
├── config/                  # Configuration files (excluded from version control)
├── logs/                    # Log files (excluded from version control)
└── .gitignore               # Git ignore file
```

> **Note**: The `config/` and `logs/` directories are excluded from version control.

## Prerequisites

- Python 3.x
- Google Cloud Platform account with BigQuery access
- Apple Health data export (XML format)


## Usage

1. Export your Apple Health data:
   - Open the Health app on your iPhone
   - Go to your profile
   - Select "Export All Health Data"
   - Save the export file

2. Run the data processing script:
```bash
python scripts/refresh.py
```

The script will:
- Process the health data export
- Calculate daily metrics
- Upload the processed data to BigQuery

## Data Processing

The pipeline processes the following metrics:

### Health Metrics
- Active Calories Burned
- Basal Calories Burned
- Total Calories Burned
- Resting Heart Rate
- Sleep Analysis (Core, Deep, REM)

### Workout Metrics
- Workout Duration
- Distance (for running)
- Elevation Gain
- Workout Type Indicators

## Configuration

Configuration is managed through:
- `src/config.py`: Main configuration file
- `config/` directory: Additional configuration files

## Logging

Logs are stored in the `logs/` directory with the following format:
- Daily log files
- Error tracking
- Processing statistics
