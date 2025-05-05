FHIR Data Pipeline for HRV Analysis
This Python-based pipeline fetches, validates, processes, and analyzes healthcare data from a FHIR (Fast Healthcare Interoperability Resources) server, with a focus on Heart Rate Variability (HRV) observations. It authenticates with a FHIR server, retrieves resources like Patient and Observation, validates data, generates exploration reports, produces a machine learning-ready table, and creates interactive biosignal visualizations.
Features

Authentication: Uses Azure credentials to securely access a FHIR server.
Data Fetching: Retrieves FHIR resources with HRV-specific queries (LOINC code 80404-7).
Validation: Checks data integrity and reports errors.
Processing: Converts FHIR data into structured formats for analysis.
Visualization: Generates interactive HRV biosignal plots.
Outputs: Saves raw data, exploration reports, and ML tables.

Prerequisites

Python 3.8+
Access to a FHIR server (e.g., Azure Health Data Services)
Azure credentials (client ID, tenant ID, client secret)
A var.env file with environment variables

Installation

Clone the Repository (if applicable):
git clone <repository-url>
cd <repository-directory>


Set Up a Virtual Environment:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate


Install Dependencies: Create a requirements.txt file with:
python-dotenv==1.0.0
pandas==2.0.3
requests==2.31.0

Install dependencies:
pip install -r requirements.txt

Note: Ensure custom modules (load_data_FHIR, fhir_preprocessor) and their dependencies (e.g., Plotly for visualization) are available.

Configure Environment Variables: Create a var.env file in the project root:
FHIR_SERVER_URL=https://<your-fhir-server>.fhir.azurehealthcareapis.com
CLIENT_ID=<your-azure-client-id>
TENANT_ID=<your-azure-tenant-id>
CLIENT_SECRET=<your-azure-client-secret>
MAX_PAGES=3

Replace placeholders with your FHIR server and Azure details.


Usage

Run the Pipeline:
source venv/bin/activate  # On Windows: venv\Scripts\activate
python main.py


Outputs: Files are saved in the fhir_analysis_HRV/ directory:



File
Description



raw_fhir_data.json
Raw FHIR resources fetched from the server


ml_table.csv
Processed data for machine learning


exploration_report.txt
Data exploration summary


hr_event_*.html
Interactive biosignal visualizations


main.log
Pipeline logs (in project root)



Customization:

Resource Types: Edit FHIR_RESOURCES in constants.py.
Query Parameters: Modify custom_params in main.py (e.g., add date filters).
Pagination: Adjust MAX_PAGES in var.env to fetch more data.



Project Structure
├── main.py                    # Main pipeline

