# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2025-01-01

### Added
- Data collection & simulation module
- Data quality analyzer (completeness, accuracy, consistency)
- Data validation with schemas and custom business rules
- Preprocessing (cleaning + normalization)
- Anomaly detection: Isolation Forest, Statistical, Seasonal, Ensemble
- Notification system (email + log alerts)
- HTML report generation
- Interactive dashboard (Dash)
- Multi-environment configuration manager (dev/staging/prod)
- Encrypted secrets manager
- Structured JSON logging with rotation and analysis
- Health monitoring and system metrics
- Feature toggle system
- CLI tools: `run_pipeline.py`, `report_tool.py`, `log_tool.py`, `config_tool.py`, `monitor.py`, `validate.py`
- Data generator for testing
- Unit tests across all core modules
- Docker support
- CI workflow (GitHub Actions)
- Makefile for common tasks

### Security
- Non-root Docker user
- Encrypted secrets at rest
- `.env` and `secrets.yaml` excluded from version control