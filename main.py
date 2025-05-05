# main.py
import logging
from load_data_FHIR import get_fhir_token # Import the function 
from fhir_preprocessor import *
from load_data_FHIR import * 
from dotenv import load_dotenv
import os
from constants import FHIR_RESOURCES
from typing import List, Dict

# Set up logging
logging.basicConfig(filename='main.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
## Load environment variables from .env file (if it exists)
load_dotenv('var.env')
print("VAULT_URL from environment:", os.getenv('VAULT_URL'))
print("SECRET_NAMES from environment:", os.getenv('SECRET_NAMES'))
print("AZURE_CLIENT_ID from environment:", os.getenv('CLIENT_ID'))
print("AZURE_TENANT_ID from environment:", os.getenv('TENANT_ID'))
print("AZURE_TENANT_ID from environment:", os.getenv('CLIENT_SECRET')) 
print("AZURE_FHIR_SERVER_URL from environment:", os.getenv('FHIR_SERVER_URL'))
#FHIR_SERVER_URL = "https://dataworkspace-fhirdata.fhir.azurehealthcareapis.com"
# Main execution block
if __name__ == "__main__":
    try:
        # Define output directory
        output_dir = "fhir_analysis_HRV"
        os.makedirs(output_dir, exist_ok=True)
        logger.info("Starting FHIR data pipeline...")

        # Step 1: Authenticate and get token
        token = get_fhir_token()
        fhir_server_url = os.getenv("FHIR_SERVER_URL")
        logger.info(f"Authenticated with FHIR server: {fhir_server_url}")


        # Step 2: Fetch FHIR data
        all_resources = []
        for resource_type in FHIR_RESOURCES:
            if resource_type == "Observation":
                # Define custom_params for HRV LOINC code and sorting
                custom_params = {
                "code": "http://loinc.org|80404-7",
                "_sort": "date",
                "_count" :1000}
            else : 
                custom_params =  {
                                    "_count" :1000}
            logger.info(f"Fetching {resource_type} resources...")
            resources = fetch_fhir_data(
                base_url=fhir_server_url,
                access_token=token,
                custom_params=custom_params,
                resource_type=resource_type,
                max_pages=3
            )
            all_resources.extend(resources)
            logger.info(f"Fetched {len(resources)} {resource_type} resources")
        
        # Step 3: Validate data
        if not all_resources:
            raise ValueError("No FHIR resources fetched")
        is_valid, errors = validate_fhir_data(all_resources)

        if errors:
            print(f"Found {len(errors)} issues:")
        for i, error in enumerate(errors[:10]):  # Show first 10 errors
            print(f"{i+1}. {error}")
    
        # Continue analysis with potentially invalid data
        df = pd.DataFrame({'errors': errors})
        print("\nError frequency:")
        print(df['errors'].value_counts().head())
        logger.info("FHIR data validated successfully")

        print(f"Resources List:{len(all_resources)}")
        # Step 4: Save raw data
        raw_data_path = os.path.join(output_dir, "raw_fhir_data.json")
        print(f"Saving raw FHIR data to {raw_data_path}...")
        # Save the raw data to a JSON file
        save_fhir_data(all_resources, raw_data_path)
        logger.info(f"Raw FHIR data saved to {raw_data_path}")
         
        
        # Step 5: Ingest data and Explore data
        exploration = explore_fhir_data(ingest_fhir_data(str(raw_data_path)))
        exploration_report_path = os.path.join(output_dir, "exploration_report.txt")
        generate_exploration_report(exploration, exploration_report_path)
        logger.info(f"Exploration report saved to {exploration_report_path}")
        logger.info(f"str(raw_data_path) {str(raw_data_path)}")
        # Step 6: Process data
        result = process_fhir_bundle(str(raw_data_path), output_dir=output_dir)
        logger.info("Data processing completed")

        # Preprocess and visualize
        # Step 7: Visualize biosignals interactively
        logger.info("Generating interactive biosignal plots...")
        visualize_biosignals_interactive(result["observations"], output_dir=output_dir)
        logger.info("Interactive visualization completed")

        #This is the part about alignnment with event triggers
        
        # Step 7: Save ML table
        ml_table_path = os.path.join(output_dir, "ml_table.csv")
        result["ml_table"].to_csv(ml_table_path, index=False)
        logger.info(f"ML table saved to {ml_table_path}")
        '''
        # Step 8: Summarize results
        logger.info("Pipeline completed successfully")
        logger.info(f"Patients processed: {len(result['patients'])}")
        logger.info(f"Observations processed: {len(result['observations'])}")
        logger.info(f"Conditions processed: {len(result['conditions'])}")
        visualization_files = [f for f in os.listdir(output_dir) if f.startswith("hr_event_")]
        logger.info(f"Visualizations generated: {len(visualization_files)}")
        '''
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
