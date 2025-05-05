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
from dateutil.parser import parse  
import datetime
import re




# Set up logging
logging.basicConfig(
    filename="data_pipeline.log",
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

def parse_date(date_str:str)->datetime:
    """
    Parse a date string into a datetime object.
    
    Args:
        date_str: Date string to parse
    
    Returns:
        Parsed datetime object
    """ 
    try: 
        return parse(date_str)
    except ValueError as e: 
        logging.error(f"Failed to parse date: {str(e)}")
        raise
    except ValueError as e:
        logging.error(f"Failed to parse date: {str(e)}")
        raise 

def validate_fhir_data(data: List[Dict]) -> Tuple[bool, List[str]]:
    """
    Validate FHIR data before saving.
    
    Args:
        data: List of FHIR resources to validate
    
    Returns:
        Tuple of (is_valid: bool, errors: List[str]) 
    """
    errors = []
    is_valid = True
    
    if not isinstance(data, list):
        errors.append("Data must be a list")
        return (False, errors)
    if len(data) == 0:
        errors.append("Data must be a non-empty list")
        return (False, errors)
        
    for idx, resource in enumerate(data):
        # Resource level checks
        if not isinstance(resource, dict):
            errors.append(f"Resource at index {idx} is not a dictionary")
            continue  # Continue to next resource
            
        if "resourceType" not in resource:
            errors.append(f"Resource at index {idx} does not have a resourceType")
            continue
            
        resource_type = resource.get("resourceType")
        
        # Patient-specific validation
        if resource_type == "Patient":
            if "id" not in resource:
                errors.append(f"Patient resource at index {idx} does not have an id")
            if "name" not in resource:
                errors.append(f"Patient resource at index {idx} does not have a name")
            if "birthDate" in resource:
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", resource["birthDate"]):
                    errors.append(f"Invalid birthDate format at index {idx}: {resource['birthDate']}")
        
        # Healthcare-Specific Checks
        if "identifier" in resource:
            if not isinstance(resource["identifier"], list):
                errors.append(f"Identifier at index {idx} is not a list")
            else:
                for ident in resource["identifier"]:
                    if "system" not in ident:
                        errors.append(f"Identifier at index {idx} does not have a system")
                    if "value" not in ident:
                        errors.append(f"Identifier at index {idx} does not have a value")
        
        # Code Validation
        if resource_type in ("Observation", "Condition"):
            if "code" not in resource or "coding" not in resource["code"]:
                errors.append(f"{resource_type} at index {idx} missing 'code.coding'")
            else:
                coding = resource["code"]["coding"][0]
                if "system" not in coding or coding["system"] not in VALID_CODE_SYSTEMS:
                    errors.append(f"Invalid code system at index {idx}: {coding.get('system', 'missing')}")
                if "code" not in coding or not coding["code"]:
                    errors.append(f"Empty or missing code value at index {idx}")
        
        # Reference Integrity
        if "subject" in resource:
            reference = resource["subject"].get("reference", "")
            if not (reference.startswith("Patient/") or reference.startswith("http")):
                errors.append(f"Invalid subject reference at index {idx}: {reference}")
        
        # Temporal Consistency
        if "birthDate" in resource and "deceasedDateTime" in resource:
            try:
                birth = parse_date(resource["birthDate"])
                death = parse_date(resource["deceasedDateTime"])
                if birth >= death:
                    errors.append(f"Death date {death} precedes birth date {birth} at index {idx}")
            except ValueError:
                errors.append(f"Invalid date format at index {idx}")
    
    is_valid = len(errors) == 0
    if is_valid:
        logging.info("Data validation successful")
    else:
        logging.warning(f"Validation found {len(errors)} issues")
    
    return (is_valid, errors)
    

    
def save_fhir_data(data:List[Dict], file_path:str)->None:
    """
    Save FHIR data to a JSON file.
    
    Args:
        data: List of FHIR resources to save
        file_path: Path to the output JSON file
    
    Returns:
        None
    """ 
    try:
        with open(file_path, "w") as f:
            json.dump(data,f, indent=4,cls=FHIREncoder)
        logging.info(f"Data saved to{file_path}")
    except Exception as e:
        logging.error(f"Error saving data to {file_path}:{str(e)}")
        raise
  
