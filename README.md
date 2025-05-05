# FHIR Data Pipeline for HRV Analysis

A Python-based pipeline to fetch, validate, process, and analyze Heart Rate Variability (HRV) data from FHIR servers.

## Features
- **Authentication**: Secure access using Azure credentials
- **Data Fetching**: Retrieves FHIR resources (Patient/Observation) with HRV-specific queries (LOINC `80404-7`)
- **Validation**: Data integrity checks and error reporting
- **Processing**: Converts FHIR data to structured formats
- **Visualization**: Interactive HRV biosignal plots
- **Outputs**: Saves raw data, reports, and ML-ready tables

## Prerequisites
- Python 3.8+
- Access to a FHIR server (e.g., Azure Health Data Services)
- Azure credentials:
  - Client ID
  - Tenant ID
  - Client secret
- `var.env` file for environment variables

## Installation

1. **Clone Repository** (if applicable):
   ```bash
   git clone <repository_url>
   cd <project_directory>
