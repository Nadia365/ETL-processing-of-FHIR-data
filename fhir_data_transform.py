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
  
