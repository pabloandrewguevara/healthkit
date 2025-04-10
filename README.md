# HealthKit Data Processing

A Python-based data processing pipeline for Apple HealthKit data export. This project processes health and workout data from Apple Health exports, transforming it into a structured format suitable for analysis and visualization.

## Project Structure

```
healthkit/
├── src/                    # Core application code
│   ├── data_processing.py  # Health and workout data processing logic
│   └── config.py          # Configuration management
├── utils/                  # Utility modules
│   ├── bigquery_utils.py  # BigQuery data upload utilities
│   └── logging_config.py  # Logging configuration
├── scripts/               # Standalone scripts
│   └── refresh.py        # Main script for data processing
├── config/               # Configuration files
├── logs/                # Log files
└── .gitignore          # Git ignore file
```

## Features

- Processes Apple HealthKit export data (XML format)
- Extracts and processes health metrics including:
  - Sleep analysis (Core, Deep, REM sleep)
  - Calorie tracking (Active, Basal, Total)
  - Heart rate data
  - Workout data
- Supports various workout types:
  - Strength Training
  - Running
  - HIIT
  - Core Training
- Calculates daily and weekly aggregates
- Exports processed data to BigQuery

## Prerequisites

- Python 3.x
- Google Cloud Platform account with BigQuery access
- Apple Health data export (XML format)

## Installation

1. Clone the repository:
```bash
git clone [repository-url]
cd healthkit
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Cloud credentials:
- Create a service account in Google Cloud Console
- Download the JSON key file
- Set the environment variable:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/service-account-key.json"
```

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

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your license information here]

## Contact

[Add your contact information here] 