# Livestock Disease Outbreak Detection System - MVP

An offline system for detecting disease outbreaks in rural livestock using anomaly detection and time series analysis.

## Features

- Offline-first architecture (no internet required)
- Main anomaly detection orchestrator
- Statistical anomaly detection
- Outbreak cluster identification
- Simple web dashboard
- SQLite database for data storage
- Data simulation for testing
  -Data cleaning and validation
  -Detection feature
  -Isolation forest feat for data analysis
  -Seasonal pattern handling and decomposition
  -Ensemble anomaly detection combining multiple methods
  Seasonal pattern handling and decomposition feature.

## Installation

```bash
# 1. Clone the repository
git clone <repository-url>
cd livestock_outbreak_detection

# 2. Run setup
python deploy.py

# 3. Install dependencies (if not done by deploy.py)
pip install -r requirements.txt
```
