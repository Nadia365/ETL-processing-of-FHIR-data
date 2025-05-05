import os
import statistics
import pandas as pd
import numpy as np
from datetime import datetime
import requests
from sklearn.preprocessing import MinMaxScaler
import logging
import matplotlib.pyplot as plt
import json
from typing import List, Dict, Any, Optional, Tuple
from constants import FHIR_RESOURCES, BIOSIGNAL_RULES
from collections import defaultdict, Counter
from statistics import median
import dask.dataframe as dd
import plotly.graph_objects as go
from dask.diagnostics import ProgressBar
import pytz
import matplotlib

matplotlib.use("Agg")  # Use non-interactive backend for plotting

# Set up logging
logging.basicConfig(
    filename="data_pipeline_HRV.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__) 
                           
# Step 1: Ingestion of FHIR data
def ingest_fhir_data(file_path: str) -> List[Dict[str, Any]]:
    """
    Load FHIR data from either:
    - A direct list of resources, or
    - A FHIR Bundle with entries
    
    Args:
        file_path: Path to FHIR JSON file
    Returns:
        List of FHIR resource dictionaries
    Raises:
        ValueError: If file is invalid or no resources found
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
            
        # Case 1: Direct list of resources
        if isinstance(data, list):
            resources = [
                resource for resource in data 
                if isinstance(resource, dict) and 
                resource.get("resourceType") in FHIR_RESOURCES
            ]
        # Case 2: FHIR Bundle format
        elif isinstance(data, dict):
            resources = [
                entry["resource"] for entry in data.get("entry", []) 
                if isinstance(entry, dict) and 
                isinstance(entry.get("resource"), dict) and
                entry["resource"].get("resourceType") in FHIR_RESOURCES
            ]
        else:
            raise ValueError("Invalid FHIR data format")
            
        if not resources:
            raise ValueError("No valid FHIR resources found")
        return resources
            
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        raise ValueError(f"Invalid FHIR data: {str(e)}")


# Step 2: Exploration of dataset
def explore_fhir_data(fhir_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Explore FHIR data and return a summary of its structure and content.

    Args:
        fhir_data (List[Dict[str, Any]]): List of FHIR resources.
    Returns:
        Dict[str, Any]: Summary of resource counts, field presence, and value distributions.
    """
    resource_counts = Counter(resource["resourceType"] for resource in fhir_data)
    exploration = {"resource_counts": dict(resource_counts), "details": {}}

    for resource_type in resource_counts.keys():
        resources = [r for r in fhir_data if r["resourceType"] == resource_type]
        exploration["details"][resource_type] = explore_resource_type(resources)

    return exploration
#More enhanced exploration resource type 
def explore_resource_type(resources: List[Dict[str, Any]]) -> Dict[str, Any]:
    field_analysis = defaultdict(lambda: {
        'presence_count': 0,
        'types': defaultdict(int),
        'values': set(),
        'nested_fields': defaultdict(lambda: defaultdict(int))
    })

    for resource in resources:
        for field, value in resource.items():
            field_data = field_analysis[field]
            field_data['presence_count'] += 1
            
            # Track value types
            field_data['types'][type(value).__name__] += 1
            
            # Handle different value types
            if isinstance(value, (str, int, float, bool)):
                field_data['values'].add(value)
            elif isinstance(value, dict):
                for nested_field, nested_value in value.items():
                    field_data['nested_fields'][nested_field][type(nested_value).__name__] += 1
            elif isinstance(value, list):
                field_data['types']['list_length'] = len(value)
                for item in value:
                    if isinstance(item, dict):
                        for nested_field in item.keys():
                            field_data['nested_fields'][nested_field]['in_list'] += 1

    # Calculate statistics for numeric fields
    numeric_stats = {}
    for field in field_analysis:
        numeric_values = [
            val for val in field_analysis[field]['values'] 
            if isinstance(val, (int, float))
        ]
        if numeric_values:
            numeric_stats[field] = {
                'min': min(numeric_values),
                'max': max(numeric_values),
                'mean': sum(numeric_values) / len(numeric_values),
                'std_dev': statistics.stdev(numeric_values) if len(numeric_values) > 1 else None
            }

    return {
        'field_analysis': field_analysis,
        'numeric_stats': numeric_stats,
        'total_resources': len(resources)
    }
          
def generate_exploration_report(exploration: Dict[str, Any], output_path: str) -> None:
    """
    Generate a text report from the updated exploration summary.
    
    Args:
        exploration (Dict[str, Any]): Exploration results from the updated explore_fhir_data()
        output_path (str): Path to save the report.
    """
    with open(output_path, "w") as f:
        f.write("FHIR Data Exploration Report\n")
        f.write("===========================\n\n")

        # Resource counts section
        f.write("Resource Counts:\n")
        for resource, count in exploration["resource_counts"].items():
            f.write(f"- {resource}: {count}\n")
        f.write("\n")

        # Details for each resource type
        for resource_type, details in exploration["details"].items():
            f.write(f"{resource_type} Details:\n")
            f.write(f"  Total Resources: {details['total_resources']}\n\n")
            
            # Field presence analysis
            f.write("  Field Presence Analysis:\n")
            for field, stats in details['field_analysis'].items():
                presence_pct = (stats['presence_count'] / details['total_resources']) * 100
                f.write(f"    - {field}: {presence_pct:.1f}% presence\n")
                f.write(f"      Types: {dict(stats['types'])}\n")
                
                # Show sample values for non-sensitive fields
                if field not in ['id', 'identifier', 'reference'] and stats['values']:
                    sample_values = list(stats['values'])[:3]
                    f.write(f"      Sample values: {sample_values}\n")
                
                # Nested fields information
                if stats['nested_fields']:
                    f.write("      Nested fields:\n")
                    for nested_field, nested_types in stats['nested_fields'].items():
                        f.write(f"        - {nested_field}: {dict(nested_types)}\n")
            f.write("\n")
            
            # Numeric statistics
            if details['numeric_stats']:
                f.write("  Numeric Field Statistics:\n")
                for field, stats in details['numeric_stats'].items():
                    f.write(f"    - {field}:\n")
                    f.write(f"      Min: {stats['min']:.2f}\n")
                    f.write(f"      Max: {stats['max']:.2f}\n")
                    f.write(f"      Mean: {stats['mean']:.2f}\n")
                    if stats['std_dev'] is not None:
                        f.write(f"      Std Dev: {stats['std_dev']:.2f}\n")
            f.write("\n")

def validate_and_clean_patient(resource: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and clean Patient resource.

    Args:
        resource (Dict[str, Any]): FHIR Patient resource.
    Returns:
        Dict[str, Any]: Cleaned Patient data.
    """
    if "id" not in resource:
        raise ValueError("Patient resource missing 'id'")
    return {
        "id": resource.get("identifier", [{}])[0].get("value", "unknown"),
        "name": resource.get("name", [{}])[0].get("text", "unknown"),
        "gender": resource.get("gender", "unknown"),
        "birthDate": resource.get("birthDate", None),
    }

def validate_and_clean_condition(
    resource: Dict[str, Any], patients: Dict[str, Dict]
) -> Optional[Dict[str, Any]]:
    """
    Validate and clean Condition resource.

    Args:
        resource (Dict[str, Any]): FHIR Condition resource.
        patients (Dict[str, Dict]): Dictionary of cleaned patient data.
    Returns:
        Optional[Dict[str, Any]]: Cleaned Condition data, or None if invalid.
    """
    patient_id = (
        resource.get("subject", {}).get("reference", "").replace("Patient/", "")
    )
    if patient_id not in patients:
        logger.warning(f"Condition for unknown patient {patient_id}")
        return None
    if "code" not in resource:
        logger.warning(f"Condition missing code for {patient_id}")
        return None
    condition_code = (
        resource["code"]["coding"][0]["code"]
        if resource["code"].get("coding")
        else "unknown"
    )
    onset = resource.get("onsetDateTime")
    if onset:
        try:
            onset = pd.to_datetime(onset, utc=True).isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Invalid onsetDateTime for {patient_id}, using current time")
            onset = datetime.now(pytz.UTC).isoformat()
    else:
        logger.warning(f"Missing onsetDateTime for {patient_id}, using current time")
        onset = datetime.now(pytz.UTC).isoformat()
    return {"patient_id": patient_id, "condition": condition_code, "onset": onset}

def validate_and_clean_observation(
    resource: Dict[str, Any], patients: Dict[str, Dict]
) -> Optional[Dict[str, Any]]:
    """
    Validate and clean Observation resource, focusing on biosignals.

    Args:
        resource (Dict[str, Any]): FHIR Observation resource.
        patients (Dict[str, Dict]): Dictionary of cleaned patient data.
    Returns:
        Optional[Dict[str, Any]]: Cleaned Observation data, or None if invalid.
    """
    try:
        # Extract patient_id
        patient_id = resource.get("subject", {}).get("reference", "").replace("Patient/", "")
        logger.warning(f"Patient id {patient_id}")
        if not patient_id or patient_id not in patients:
            logger.warning(f"Invalid or missing patient reference in Observation")
            return None

        # Extract code from coding
        code_info = resource.get("code", {}).get("coding", [])
        if not code_info:
            logger.warning(f"Observation missing code for patient {patient_id}")
            return None
        code = code_info[0].get("code", "UNKNOWN")
        logger.debug(f"Processing Observation with code {code} for patient {patient_id}")

        # Extract value and unit
        value_quantity = resource.get("valueQuantity", {})
        value = value_quantity.get("value")
        unit = value_quantity.get("unit")
        if value is None:
            logger.warning(f"Observation missing value for patient {patient_id}, code {code}")
            return None

        # Extract timestamp
        timestamp = resource.get("effectiveDateTime")
        if not timestamp:
            logger.warning(f"Missing effectiveDateTime for Observation {patient_id}, code {code}")
            timestamp = datetime.now(pytz.UTC).isoformat()
        try:
            timestamp = pd.to_datetime(timestamp, utc=True).isoformat()
        except (ValueError, TypeError):
            logger.warning(f"Invalid effectiveDateTime for Observation {patient_id}, using current time")
            timestamp = datetime.now(pytz.UTC).isoformat()

        # Apply BIOSIGNAL_RULES
        if code in BIOSIGNAL_RULES:
            rules = BIOSIGNAL_RULES[code]
            if not isinstance(value, (int, float)):
                logger.warning(f"Invalid value type for {code} in Observation {patient_id}")
                return None
            if unit and unit != rules["unit"]:
                value = convert_units(value, unit, rules["unit"])
                unit = rules["unit"]
            if value is not None and (value < rules["min"] or value > rules["max"]):
                logger.warning(f"Value {value} out of range for {code} in Observation {patient_id}")
                value = None
            if value is None:
                return None
        else:
            logger.info(f"Code {code} not in BIOSIGNAL_RULES, processing without validation")

        return {
            "patient_id": patient_id,
            "code": code,
            "value": value,
            "unit": unit,
            "timestamp": timestamp,
        }
    except Exception as e:
        logger.warning(f"Failed to process Observation for patient {patient_id}: {str(e)}")
        return None

def convert_units(value: float, from_unit: str, to_unit: str) -> Optional[float]:
    """
    Convert units for biosignals.

    Args:
        value (float): Original value.
        from_unit (str): Current unit.
        to_unit (str): Target unit.
    Returns:
        Optional[float]: Converted value, or None if conversion fails.
    """
    if from_unit == "minutes" and to_unit == "hours":
        return value / 60
    if from_unit == to_unit:
        return value
    logger.warning(f"Unsupported unit conversion from {from_unit} to {to_unit}")
    return None

            legend_title="Biosignal",
            template="plotly",
            showlegend=True
        )
        
        # Save plot as HTML
        plot_path = os.path.join(output_dir, f"biosignals_patient_{patient_id}.html")
        fig.write_html(plot_path)
        logger.info(f"Saved interactive plot for patient {patient_id} to {plot_path}")

def preprocess_biosignals(
    observations: List[Dict[str, Any]], conditions: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Preprocess biosignals: remove duplicates, impute None, treat outliers, normalize timestamps,
    and resample to 1-hour intervals.

    Args:
        observations: List of cleaned observation dictionaries.
        conditions: List of cleaned condition dictionaries.
    Returns:
        List of preprocessed observation dictionaries.
    """
    if not observations:
        logger.warning("No observations provided to preprocess_biosignals")
        return []

    df = pd.DataFrame(observations)
    if df.empty:
        logger.warning("No valid observations for preprocessing")
        return []

    logger.debug(f"Sample observations before preprocessing: {df.head(2).to_dict('records')}")

    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    except Exception as e:
        logger.error(f"Failed to convert timestamps: {str(e)}")
        return []

    df = df.drop_duplicates(subset=["patient_id", "code", "timestamp"], keep="last")

    for code in df["code"].unique():
        mask = df["code"] == code
        values = df.loc[mask & df["value"].notna(), "value"]
        if not values.empty:
            impute_value = values.median()
            df.loc[mask & df["value"].isna(), "value"] = impute_value
            logger.info(f"Imputed {code} None values with median {impute_value}")

    for code in df["code"].unique():
        mask = df["code"] == code
        values = df.loc[mask, "value"].dropna()
        if not values.empty:
            q1, q3 = values.quantile(0.25), values.quantile(0.75)
            iqr = q3 - q1
            lower_bound, upper_bound = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            df.loc[mask & (df["value"] < lower_bound), "value"] = lower_bound
            df.loc[mask & (df["value"] > upper_bound), "value"] = upper_bound
            logger.info(f"Capped {code} outliers at {lower_bound}–{upper_bound}")

    resampled_data = []
    for patient_id in df["patient_id"].unique():
        for code in df["code"].unique():
            patient_code_df = df[
                (df["patient_id"] == patient_id) & (df["code"] == code)
            ].copy()
            if patient_code_df.empty:
                continue
            patient_code_df.set_index("timestamp", inplace=True)
            resampled_df = (
                patient_code_df[["value", "unit"]]
                .resample("1H")
                .ffill()
                .reset_index()
            )
            resampled_df["patient_id"] = patient_id
            resampled_df["code"] = code
            resampled_data.append(resampled_df)

    if resampled_data:
        df = pd.concat(resampled_data, ignore_index=True)
    else:
        df = pd.DataFrame(
            columns=["patient_id", "code", "value", "unit", "timestamp"]
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

def preprocess_patients(patients: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Preprocess patient data: remove duplicates, impute None, handle timestamps.

    Args:
        patients: Dictionary of patient data.
    Returns:
        Preprocessed patient dictionary.
    """
    logger = logging.getLogger(__name__)
    
    # Create a new dictionary to store preprocessed data
    processed_patients = {}
    
    # Process each patient
    for pid, data in patients.items():
        # Skip if patient ID already processed (unlikely since dict keys are unique)
        if pid in processed_patients:
            logger.warning(f"Duplicate patient ID {pid} found, keeping first occurrence")
            continue
            
        # Create a copy of patient data
        patient_data = data.copy()
        
        # Impute None for birthDate
        if patient_data.get("birthDate") is None:
            patient_data["birthDate"] = "unknown"
            logger.info(f"Imputed birthDate for {pid} as 'unknown'")
        
        # Standardize birthDate timestamp
        try:
            patient_data["birthDate"] = pd.to_datetime(patient_data["birthDate"]).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            patient_data["birthDate"] = "unknown"
            logger.info(f"Set invalid birthDate for {pid} to 'unknown'")
        
        # Add to processed patients
        processed_patients[pid] = patient_data
    
    return processed_patients

#Update the process FHIR bundle with visualization and outputing data to .csv file
def process_fhir_bundle(fhir_json: str, output_dir: str = "fhir_analysis") -> Dict[str, Any]:
    """
    Main pipeline with preprocessing, cross-resource consistency, and visualization.

    Args:
        fhir_json: Path to FHIR JSON file.
        output_dir: Directory to save outputs.
    Returns:
        Dictionary containing processed patients, observations, conditions, ML table, and report.
    """
    resources = ingest_fhir_data(fhir_json)
    if not resources:
        return {
            "patients": {},
            "observations": [],
            "conditions": [],
            "ml_table": None,
            "report": {},
        }

    patients = {}
    observations = []
    conditions = []

    # First pass: Process all Patient resources to populate patients dictionary
    for resource in resources:
        if resource["resourceType"] == "Patient":
            cleaned = validate_and_clean_patient(resource)
            logger.info(f"Cleaned Patient: {cleaned}")
            if cleaned:
                patients[cleaned["id"]] = cleaned
                logger.info(f"Added patient to dictionary: {cleaned['id']}")
                logger.debug(f"Current patients dictionary: {patients}")

    # Second pass: Process Observation and Condition resources
    for resource in resources:
        r_type = resource["resourceType"]
        if r_type == "Observation":
            cleaned = validate_and_clean_observation(resource, patients)
            if cleaned:
                observations.append(cleaned)
                logger.debug(f"Added observation for patient {cleaned['patient_id']}")
        elif r_type == "Condition":
            cleaned = validate_and_clean_condition(resource, patients)
            if cleaned:
                conditions.append(cleaned)
                logger.debug(f"Added condition for patient {cleaned['patient_id']}")

    # Validate observations
    valid_observations = []
    for obs in observations:
        if obs["patient_id"] in patients:
            valid_observations.append(obs)
        else:
            logger.warning(
                f"Dropping Observation for non-existent Patient/{obs['patient_id']}"
            )
    observations = valid_observations
    logger.info(f"Valid Observations: {observations}")
    logger.debug(f"Observations after validation: {len(observations)}")

    # Save valid observations to CSV
    os.makedirs(output_dir, exist_ok=True)
    valid_obs_df = pd.DataFrame(valid_observations)
    valid_obs_csv = os.path.join(output_dir, "valid_observations.csv")
    valid_obs_df.to_csv(valid_obs_csv, index=False)
    logger.info(f"Saved valid observations to {valid_obs_csv}")

    # Visualize valid observations
    visualize_biosignals(
        valid_observations,
        output_dir,
        filename="raw_observations.png",
        title="Raw Biosignals Time Series"
    )

    # Preprocess biosignals
    observations = preprocess_biosignals(observations, conditions)
    logger.info(f"Sample observations after preprocessing: {observations}")

    # Save processed observations to CSV
    processed_obs_df = pd.DataFrame(observations)
    processed_obs_csv = os.path.join(output_dir, "processed_observations.csv")
    processed_obs_df.to_csv(processed_obs_csv, index=False)
    logger.info(f"Saved processed observations to {processed_obs_csv}")

    # Visualize processed observations
    visualize_biosignals(
        observations,
        output_dir,
        filename="processed_observations.png",
        title="Processed Biosignals Time Series"
    )

    patients = preprocess_patients(patients)
    logger.info(f"Sample patients after preprocessing: {patients}")
    df = pd.DataFrame(observations)
    logger.info(f"Sample observations after preprocessing: {df}")
    if df.empty:
        logger.warning("No observations after preprocessing")
        hr_stats = None
        sleep_stats = None
        hrv_stats = None
    else:
        ddf = dd.from_pandas(df, npartitions=2)
        ddf["code"] = ddf["code"].astype("str").str.strip()  # Ensure clean string comparison
        with ProgressBar():
            hr_stats = None
            sleep_stats = None
            hrv_stats = None
            unique_codes = ddf["code"].unique().compute().values
            if "8867-4" in unique_codes:
                hr_stats = ddf[ddf["code"] == "8867-4"]["value"].describe().compute()
            if "SLEEP_DURATION" in unique_codes:
                sleep_stats = ddf[ddf["code"] == "SLEEP_DURATION"]["value"].describe().compute()
            if "80404-7" in unique_codes:
                hrv_stats = ddf[ddf["code"] == "80404-7"]["value"].describe().compute()
                print(f"HRV Stats: {hrv_stats}")
            logger.info(f"HRV Stats: {hrv_stats}")
    logger.info(f"HR Stats:\n{hr_stats if hr_stats is not None else 'No HR data'}\nSleep Duration Stats:\n{sleep_stats if sleep_stats is not None else 'No sleep data'}\nHRV Stats:\n{hrv_stats if hrv_stats is not None else 'No HRV data'}")

    for code in df["code"].unique():
        mask = df["code"] == code
        values = df.loc[mask, "value"].dropna()
        if not values.empty:
            mean, std = values.mean(), values.std()
            if std > 0:
                df.loc[mask, "value"] = (df.loc[mask, "value"] - mean) / std
                logger.info(f"Normalized {code} to z-scores (mean={mean}, std={std})")

    df_data = []
    for obs in df.to_dict("records"):
        patient_id = obs["patient_id"]
        related_condition = next(
            (c for c in conditions if c["patient_id"] == patient_id), None
        )
        severity = (
            {"mild": 1, "moderate": 2, "severe": 3}.get(
                related_condition["condition"] if related_condition else "", None
            )
        )
        row = {
            "patient_id": patient_id,
            "timestamp": obs["timestamp"],
            "HR": obs["value"] if obs["code"] == "8867-4" else None,
            "sleep_duration": obs["value"] if obs["code"] == "SLEEP_DURATION" else None,
            "HRV": obs["value"] if obs["code"] == "80404-7" else None,
            "seizure_severity": severity,
            "mood": "unknown",
        }
        df_data.append(row)
    ml_table = pd.DataFrame(df_data).fillna(
        {
            "HR": pd.NA,
            "sleep_duration": pd.NA,
            "HRV": pd.NA,
            "seizure_severity": pd.NA,
        }
    )

    report = {}
    for pid in patients:
        patient_obs = [o for o in observations if o["patient_id"] == pid]
        report[pid] = {
            "name": patients[pid]["name"],
            "avg_HR": pd.Series(
                [o["value"] for o in patient_obs if o["code"] == "8867-4"]
            ).mean(),
            "total_sleep": pd.Series(
                [o["value"] for o in patient_obs if o["code"] == "SLEEP_DURATION"]
            ).sum(),
            "avg_HRV": pd.Series(
                [o["value"] for o in patient_obs if o["code"] == "80404-7"]
            ).mean(),
            "seizures": len([c for c in conditions if c["patient_id"] == pid]),
        }

    return {
        "patients": patients,
        "observations": observations,
        "conditions": conditions,
        "ml_table": ml_table,
        "report": report,
    }
