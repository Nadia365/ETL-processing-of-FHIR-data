from azure.identity import DefaultAzureCredential
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
import logging 
from dotenv import load_dotenv
import os
import json
from typing import List, Dict, Optional, Tuple
import requests
import time
from constants import PHI_FIELDS, VALID_CODE_SYSTEMS
from dateutil.parser import parse  
import datetime
import re
from json import JSONEncoder
#For very large datasets, you might want to use json.dump() with a custom encoder or stream the output:
class FHIREncoder(JSONEncoder):
    def default(self, obj):
        # Handle special FHIR types if needed
        return super().default(obj)

# Set up logging
logging.basicConfig(
    filename="data_pipeline_HRV.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Step 1: Retrieve token from FHIR server
'''
In many FHIR implementations, especially those using secure APIs, Step 1 often involves obtaining an
authentication token from the FHIR server (or an associated authorization server) to access protected 
resources. This typically happens via an OAuth 2.0 flow, such as the Client Credentials or Authorization
Code flow, depending on the setup. The token is then used in subsequent requests to authenticate and
authorize access to patient data or other resources.
'''
def get_fhir_token():
    """
    Retrieve FHIR server credentials from Azure Key Vault.
    
    Args:
        vault_url (str): URL of the Azure Key Vault (e.g., 'https://myvault.vault.azure.net').
        secret_names (dict): Dictionary of retrieved credentials (client_id, client_secret, tenant_id, fhir_url).
    
    Returns:
        dict: Dictionary of retrieved credentials.
    Raises:
        KeyError: If required environment variables are not set.
        Exception: If there's an error retrieving secrets from Key Vault.
    """
    try: 
        #Retrieve environment variables 
        VAULT_URL=os.getenv('VAULT_URL')
        secret_names_str=os.getenv('SECRET_NAMES') 
        TENANT_ID=os.getenv('TENANT_ID')
        CLIENT_ID=os.getenv('CLIENT_ID')
        CLIENT_SECRET=os.getenv('CLIENT_SECRET')
        FHIR_SERVER_URL=os.getenv('FHIR_SERVER_URL')
        
        #Validate environment variables
        if not VAULT_URL:
            raise KeyError("Environment variable 'VAULT_URL' is not set")
        if not TENANT_ID:
            raise KeyError("Environment variable 'Tenant id' is not set") 
        if not CLIENT_ID:
            raise KeyError("Environment variable 'Client id' is not set") 
        if not CLIENT_SECRET:
            raise KeyError("Environment variable 'Client secret' is not set")
        if not FHIR_SERVER_URL:
            raise KeyError("Environment variable 'FHIR server URL' is not set")
        # Parse secret_names (stored as a JSON string) 
        # Parse secret_names (stored as a JSON string)
        try:
            secret_names = json.loads(secret_names_str)
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing SECRET_NAMES as JSON: {str(e)}")
            raise ValueError("SECRET_NAMES must be a valid JSON string")
        
        #Initialize ClientSecretCredential
        print("Attempting to authenticate with ClientSecretCredential...")
        credential=ClientSecretCredential(tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        #secret_client=SecretClient(vault_url=vault_url, credential=credential) 
        #secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
        # Construct the scope dynamically using the FHIR server URL
        scope = f"{FHIR_SERVER_URL.rstrip('/')}/.default"
        print(f"Requesting token with scope: {scope}")
        FHIR_SERVER_AUTH_TOKEN = credential.get_token(
           scope
        ).token

        logging.info(
            f"Token retrieved: {FHIR_SERVER_AUTH_TOKEN[:10]}... and FHIR server URL: {FHIR_SERVER_URL}"
        )
        print(f"Requested token: {FHIR_SERVER_AUTH_TOKEN}")
        logging.info("Success logging to FHIR server")

        return FHIR_SERVER_AUTH_TOKEN
    except Exception as e:
        logging.error(f"Error retrieving secrets from Key Vault: {str(e)}")
        raise 


# Step 2: Load data from FHIR server
def fetch_fhir_data(
    base_url: str,
    access_token: str,
    resource_type: str = "",
    custom_params: Optional[Dict[str, str]] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None,
    max_retries: int = 3,
    retry_delay: int = 5
) -> List[Dict]:
    """
    Fetch FHIR data from a server with pagination and retry logic.
    
    Args:
        base_url: Base FHIR server URL
        access_token: Bearer token for authentication
        resource_type: Specific FHIR resource type (empty string for all)
        page_size: Number of entries per page
        max_pages: Maximum number of pages to fetch (None for all)
        max_retries: Maximum number of retries for failed requests
        retry_delay: Delay between retries in seconds
    
    Returns:
        List of FHIR resource entries
    """
    all_entries = []
    current_url=f"{base_url}/{resource_type}" if resource_type else base_url 
    # Initialize parameters with page size and merge with custom parameters 
    params={"_count": str(page_size)}
    if custom_params:
        params.update(custom_params) 
    headers={"Authorization":f"Bearer {access_token}", 
            "Accept":"application/fhir+json", 
            "Content-Type":"application/fhir+json"}  
    page_count=0
    while current_url:
        retries=0 
        success=False 
        while retries<max_retries and not success: 
            try: 
                logging.info(f"Fetching data {page_count+1}from: {current_url} with params: {params}(Attempt{retries+1})")
                response=requests.get (current_url, headers=headers,params=params if page_count==0 else None, 
                timeout=30 ) 
                if response.status_code != 200:
                    raise Exception(f"Failed to fetch data: {response.status_code} {response.text}")
                bundle=response.json() 
                success=True
            except(requests.exceptions.RequestException, json.JSONDecodeError) as e:
                retries+=1 
                if retries ==max_retries: 
                    logging.error(f"Max retries reached for page {page_count + 1}: {str(e)}")
                    raise Exception(f"Failed after {max_retries} retries: {str(e)}")
                logging.warning(f"Retry {retries}/{max_retries} for page {page_count + 1}: {str(e)}")
                time.sleep(retry_delay)  
        #Extract entries if they exist  
        if "entry" in bundle and bundle["entry"]:
            entries=[entry["resource"] for entry in bundle["entry"]]
            all_entries.extend(entries) 
            logging.info(f"Fetched {len(entries)} entries from page {page_count + 1}")  
            #
        #Handle pagination 
        next_url=None
        if "link" in bundle:
            for link in bundle["link"]:
                if link["relation"] == "next":
                    next_url=link["url"]
                    break
        current_url=next_url
        page_count+=1 
        if max_pages and page_count >= max_pages:
            logging.info(f"Reached maximum page limit of {max_pages}")
            break
          
    logging.info(f"Completed fetching. Total entries: {len(all_entries)}")
    return all_entries
