FHIR Data Pipeline for HRV Analysis
This project is a Python-based pipeline for interacting with a FHIR (Fast Healthcare Interoperability Resources) server to fetch, validate, process, and analyze healthcare data, specifically focusing on Heart Rate Variability (HRV) observations. The pipeline authenticates with a FHIR server, retrieves data for specified resource types (e.g., Patient, Observation), validates the data, generates exploration reports, processes it into a machine learning-ready table, and creates interactive biosignal visualizations.
Features

Authentication: Securely authenticates with a FHIR server using Azure credentials.
Data Fetching: Retrieves FHIR resources (e.g., Observation, Patient) with customizable parameters, including HRV-specific queries (LOINC code 80404-7).
Validation: Validates fetched FHIR data and reports errors.
Data Processing: Ingests, explores, and processes FHIR data into structured formats.
Visualization: Generates interactive biosignal plots for HRV observations.
Output: Saves raw data, exploration reports, and a machine learning table (ml_table.csv).

Prerequisites

Python 3.8 or higher
A FHIR server (e.g., Azure Health Data Services FHIR server) with accessible credentials
An Azure account with client ID, tenant ID, and client secret for authentication
A .env file (var.env) with required environment variables

Setup

Clone the Repository (if applicable):
git clone <repository-url>
cd <repository-directory>


Install Dependencies:Create a virtual environment and install required Python packages:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

Example requirements.txt:
python-dotenv==1.0.0
pandas==2.0.3
requests==2.31.0

Note: Additional dependencies (e.g., for fhir_preprocessor, visualization libraries) may be required based on the imported modules (load_data_FHIR, fhir_preprocessor). Ensure these are installed or provided.

Configure Environment Variables:Create a var.env file in the project root with the following variables:
FHIR_SERVER_URL=https://<your-fhir-server>.fhir.azurehealthcareapis.com
CLIENT_ID=<your-azure-client-id>
TENANT_ID=<your-azure-tenant-id>
CLIENT_SECRET=<your-azure-client-secret>
VAULT_URL=<optional-vault-url>
SECRET_NAMES=<optional-secret-names>
MAX_PAGES=3  # Optional: Number of pages to fetch per resource type

Replace placeholders with your actual Azure and FHIR server details.

Directory Structure:Ensure the following files and directories are present:
├── main.py                # Main pipeline script
├── var.env                # Environment variables
├── constants.py           # FHIR_RESOURCES and other constants
├── load_data_FHIR.py      # FHIR authentication and data fetching functions
├── fhir_preprocessor.py   # Data processing and visualization functions
├── requirements.txt       # Python dependencies
├── fhir_analysis_HRV/     # Output directory (auto-created)
│   ├── raw_fhir_data.json # Raw FHIR data
│   ├── ml_table.csv       # Machine learning table
│   ├── exploration_report.txt # Data exploration report
│   └── *.html             # Interactive visualization files
└── main.log               # Log file



Usage

Run the Pipeline:Activate the virtual environment and execute the script:
source venv/bin/activate  # On Windows: venv\Scripts\activate
python main.py


Expected Output:

Logs: Pipeline progress and errors are logged to main.log.
Output Files:
fhir_analysis_HRV/raw_fhir_data.json: Raw FHIR resources.
fhir_analysis_HRV/exploration_report.txt: Data exploration summary.
fhir_analysis_HRV/ml_table.csv: Processed data for machine learning.
fhir_analysis_HRV/hr_event_*.html: Interactive biosignal visualizations.


Console Output: Validation errors (if any) and error frequency.


Customization:

Resource Types: Modify FHIR_RESOURCES in constants.py to include additional FHIR resources (e.g., MedicationRequest).
Query Parameters: Adjust custom_params in main.py for specific resource types (e.g., filter Observations by date range).
Max Pages: Set the MAX_PAGES environment variable to fetch more or all pages of data.



Example Workflow

The pipeline authenticates with the FHIR server using Azure credentials.
It fetches resources (e.g., Observations with HRV LOINC code 80404-7) in batches of 1000, up to 3 pages.
Data is validated, and errors are logged and printed.
Raw data is saved as JSON, explored, and processed into a machine learning table.
Interactive HRV visualizations are generated and saved as HTML files.

Troubleshooting

Authentication Errors:
Verify CLIENT_ID, TENANT_ID, CLIENT_SECRET, and FHIR_SERVER_URL in var.env.
Ensure your Azure account has access to the FHIR server.


No Data Fetched:
Check main.log for errors in fetch_fhir_data.
Ensure the FHIR server contains data for the specified resources (e.g., Observations with LOINC code 80404-7).


Missing Dependencies:
Install all required packages listed in requirements.txt.
If load_data_FHIR or fhir_preprocessor are custom modules, ensure they are available in the project directory.


Visualization Issues:
Verify that result["observations"] contains data in main.py.
Check if the visualization library (e.g., Plotly) is installed.



Security Notes

Environment Variables: Store sensitive information (e.g., CLIENT_SECRET) in var.env, not in source code. Ensure var.env is excluded from version control (e.g., add to .gitignore).
Logging: Avoid logging sensitive data (e.g., tokens, patient information) to main.log.
Console Output: The script prints environment variables for debugging. Remove these print statements in production to prevent exposure.

Future Improvements

Add support for fetching all pages of FHIR data dynamically.
Implement chunked processing for large datasets to reduce memory usage.
Enhance validation to include specific checks for HRV data integrity.
Add unit tests for critical functions (e.g., fetch_fhir_data, process_fhir_bundle).

License
This project is licensed under the MIT License. See the LICENSE file for details (if applicable).
Contact
For questions or contributions, please contact the project maintainer or open an issue in the repository.
