import json
import random
import uuid
import numpy as np
import pandas as pd
import dask.dataframe as dd
import logging
import os
import matplotlib.pyplot as plt
from typing import List, Dict
from io import StringIO
import matplotlib
matplotlib.use('Agg')  # Set backend to Agg (non-GUI) to avoid Tkinter issues

# Set random seeds for reproducibility
random.seed(42)
np.random.seed(42)

# Set up logging
logging.basicConfig(filename='main_fake_data.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to save DataFrame and log summary
def save_and_log_ddf(ddf, step_name, output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"preprocessed_hrv_{step_name}.csv")
    df_computed = ddf.compute()
    df_computed.to_csv(output_path, index=False)
    logger.info(f"Saved {step_name} to {output_path}")
    logger.info(f"Shape after {step_name}: {df_computed.shape}")
    logger.info(f"Missing values after {step_name}:\n{df_computed.isnull().sum().to_string()}")
    return df_computed

# Time-series plot function (per-patient plots)
def plot_dask_timeseries(ddf, step_name, output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    df = ddf[["patient_id", "timestamp", "hrv_value"]].compute()
    for patient_id in df["patient_id"].unique():
        if pd.isna(patient_id):
            continue
        df_patient = df[df["patient_id"] == patient_id][["timestamp", "hrv_value"]]
        if df_patient.empty:
            continue
        df_patient = df_patient.sort_values("timestamp")
        plt.figure(figsize=(12, 6))
        plt.plot(df_patient["timestamp"], df_patient["hrv_value"], marker="o", markersize=4, alpha=0.7)
        plt.title(f"HRV Values Over Time for Patient {patient_id} ({step_name})")
        plt.xlabel("Timestamp")
        plt.ylabel("HRV Value (ms)")
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        safe_patient_id = str(patient_id).replace("/", "_").replace(":", "_")
        plot_path = os.path.join(output_dir, f"hrv_plot_{step_name}_patient_{safe_patient_id}.png")
        plt.savefig(plot_path)
        plt.close()
        logger.info(f"Saved plot for {step_name} (Patient {patient_id}) to {plot_path}")

# DataFrame description function (unchanged)
def describe_dataframe(df, output_path="data/df_description.txt"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    description = []
    
    description.append("=== DataFrame Info ===")
    output = StringIO()
    df.info(buf=output)
    description.append(output.getvalue())
    description.append("\n")
    
    description.append("=== Statistical Summary ===")
    description.append(df.describe(include='all').to_string())
    description.append("\n")
    
    description.append("=== Missing Values ===")
    description.append(df.isnull().sum().to_string())
    description.append("\n")
    
    description.append("=== Unique Values ===")
    description.append(df.nunique().to_string())
    description.append("\n")
    
    if df['timestamp'].notnull().any():
        description.append("=== Timestamp Range ===")
        description.append(f"Min Timestamp: {df['timestamp'].min()}")
        description.append(f"Max Timestamp: {df['timestamp'].max()}")
        description.append("\n")
    
    with open(output_path, "w") as f:
        f.write("\n".join(description))
    logger.info(f"Saved DataFrame description to {output_path}")
    
    if df['hrv_value'].notnull().any():
        plt.figure(figsize=(8, 6))
        df['hrv_value'].hist(bins=20)
        plt.title("Distribution of HRV Values")
        plt.xlabel("HRV Value (ms)")
        plt.ylabel("Frequency")
        plt.grid(True)
        hist_path = output_path.replace(".txt", "_hrv_histogram.png")
        plt.savefig(hist_path)
        plt.close()
        logger.info(f"Saved HRV histogram to {hist_path}")

# New function to save raw DataFrame values
def save_raw_dataframe(df, output_path="data/raw_hrv_values.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df[["patient_id", "timestamp", "hrv_value"]].to_csv(output_path, index=False)
    logger.info(f"Saved raw DataFrame values to {output_path}")

# Function to save hrv_value values
def save_hrv_values(ddf, step_name, output_dir="data"):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"hrv_values_{step_name}.txt")
    hrv_values = ddf["hrv_value"].compute().tolist()
    with open(output_path, "w") as f:
        f.write("\n".join(map(str, hrv_values)))
    logger.info(f"Saved hrv_value values for {step_name} to {output_path}")
    return hrv_values

# Function to generate messy FHIR HRV data with time series per patient
def generate_messy_fhir_hrv(num_patients, obs_per_patient):
    start_date = "2025-01-01 00:00:00"
    end_date = "2025-03-31 23:55:00"
    timestamps = pd.date_range(start=start_date, end=end_date, freq="1min", tz="UTC")
    timestamps = [ts.strftime("%Y-%m-%dT%H:%M:%SZ") for ts in timestamps]
    
    fhir_resources = []
    for _ in range(num_patients):
        patient_id = str(uuid.uuid4())
        start_idx = random.randint(0, len(timestamps) - obs_per_patient)
        patient_timestamps = timestamps[start_idx:start_idx + obs_per_patient]
        
        for ts in patient_timestamps:
            resource = {
                "resourceType": "Observation",
                "id": str(uuid.uuid4()),
                "status": random.choice(["final", "preliminary", "Final", "unknown", ""]),
                "category": [
                    {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                "code": random.choice(["vital-signs", "VitalSigns", "vitals", ""]),
                                "display": random.choice(["Vital Signs", "Vitals", "vital signs", ""])
                            }
                        ]
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": random.choice(["8867-4", "HRV-123", "88674", ""]),
                            "display": random.choice(["Heart rate variability", "HRV", "Heart Variability", ""])
                        }
                    ]
                },
                "subject": {
                    "reference": f"Patient/{patient_id}",
                    "display": random.choice(["Patient Name", "", "John Doe", None])
                },
                "effectiveDateTime": ts,
                "valueQuantity": {
                    "value": random.uniform(20, 100) if random.random() > 0.3 else None,
                    "unit": random.choice(["ms", "milliseconds", "MS", "", None]),
                    "system": "http://unitsofmeasure.org" if random.random() > 0.4 else ""
                },
                "note": random.choice([None, "Messy data entry", "HRV recorded", "typo in recrd", ""])
            }

            fhir_resources.append(resource)
    
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, "messy_fhir_hrv.json"), "w") as f:
        json.dump({"entry": fhir_resources}, f, indent=2)
    logger.info(f"Saved raw FHIR resources to {os.path.join(output_dir, 'messy_fhir_hrv.json')}")
    
    return fhir_resources

# Function to convert FHIR data to DataFrame and preprocess
def preprocess_fhir_hrv(fhir_resources):
    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Step 1: Convert FHIR resources to DataFrame
    data = []
    for resource in fhir_resources:
        try:
            patient_id = resource.get("subject", {}).get("reference", "").replace("Patient/", "") or None
            value = resource.get("valueQuantity", {}).get("value", None)
            timestamp = resource.get("effectiveDateTime", None)
            data.append({"patient_id": patient_id, "hrv_value": value, "timestamp": timestamp})
        except (AttributeError, TypeError):
            continue
    
    df = pd.DataFrame(data)
    # Save raw DataFrame values
    save_raw_dataframe(df, os.path.join(output_dir, "raw_hrv_values.csv"))
    # Generate DataFrame description
    describe_dataframe(df, os.path.join(output_dir, "df_description.txt"))
    # Sort by patient_id and timestamp
    df = df.sort_values(["patient_id", "timestamp"])
    ddf = dd.from_pandas(df, npartitions=4)
    save_and_log_ddf(ddf, "step1_initial")
    plot_dask_timeseries(ddf, "step1_initial")
    
    # Step 2: Remove duplicates
    ddf = ddf.drop_duplicates()
    save_and_log_ddf(ddf, "step2_deduplicated")
    plot_dask_timeseries(ddf, "step2_deduplicated")
    
    # Step 3: Impute None values
    ddf["patient_id"] = ddf["patient_id"].fillna("unknown")
    median_hrv = ddf["hrv_value"].median_approximate().compute()
    ddf["hrv_value"] = ddf["hrv_value"].fillna(median_hrv)
    save_and_log_ddf(ddf, "step3_imputed")
    plot_dask_timeseries(ddf, "step3_imputed")
    
    # Step 4: Normalize timestamps
    def parse_timestamp(ts):
        if not ts or ts == "invalid-date":
            return pd.NaT
        try:
            for fmt in ["%Y-%m-%dT%H:%M:%SZ", "%Y/%m/%d %H:%M:%S", "%m-%d-%Y %H:%M"]:
                try:
                    return pd.to_datetime(ts, format=fmt, utc=True)
                except ValueError:
                    continue
            return pd.to_datetime(ts, utc=True, errors="coerce")
        except:
            return pd.NaT
    
    ddf["timestamp"] = ddf["timestamp"].map(parse_timestamp, meta=("timestamp", "datetime64[ns, UTC]"))
    #default_timestamp = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    #ddf["timestamp"] = ddf["timestamp"].fillna(default_timestamp)
    ddf["timestamp"] = ddf["timestamp"].astype("datetime64[ns, UTC]")
    save_and_log_ddf(ddf, "step4_timestamp_normalized")
    plot_dask_timeseries(ddf, "step4_timestamp_normalized")
    
    print("Values after processing and before outliers capping",ddf["hrv_value"].compute().values )
    # Step 5: Treat outliers in hrv_value
    q1 = ddf["hrv_value"].quantile(0.25).compute()
    q3 = ddf["hrv_value"].quantile(0.75).compute()
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    print("Lower bound:", lower_bound)
    print("Upper bound:", upper_bound)
    
    def cap_outliers(series, lower, upper):
        return series.clip(lower=lower, upper=upper)
    
    ddf["hrv_value"] = ddf["hrv_value"].map_partitions(
        lambda s: cap_outliers(s, lower_bound, upper_bound), meta=("hrv_value", "float64")
    )
    save_and_log_ddf(ddf, "step5_outliers_capped")
    plot_dask_timeseries(ddf, "step5_outliers_capped")
    
    # Step 6: Resample to 1-minute intervals using pandas
    df = ddf.compute()
    df = df.set_index("timestamp")
    df = df.groupby("patient_id").resample("5min").agg({"hrv_value": "mean"})
    df = df.reset_index()
    df["patient_id"] = df["patient_id"].fillna("unknown")
    df["hrv_value"] = df["hrv_value"].fillna(median_hrv)
    ddf = dd.from_pandas(df, npartitions=4)
    save_and_log_ddf(ddf, "step6_resampled")
    plot_dask_timeseries(ddf, "step6_resampled")
    
    # Step 7: Prepare for ML
    ddf["hrv_value"] = ddf["hrv_value"].astype(float)
    save_and_log_ddf(ddf, "step7_final")
    plot_dask_timeseries(ddf, "step7_final")
    
    # Save and print hrv_value values
    hrv_values = save_hrv_values(ddf, "step7_final")
    print(f"Sample of hrv_value values (first 5) for step7_final:")
    print(hrv_values[:5])
    
    return ddf

# Main execution block
if __name__ == "__main__":
    try:
        output_dir = "fhir_analysis_HRV_fake"
        os.makedirs(output_dir, exist_ok=True)
        logger.info("Starting FHIR data pipeline...")
        
        # Step 1: Generate FHIR fake data
        fhir_resources = generate_messy_fhir_hrv(num_patients=1, obs_per_patient=1000)
        print("Generated FHIR Resources:")
        print(json.dumps({"entry": fhir_resources[:5]}, indent=2))
        
        # Step 2: Validate data (placeholder)
        patient_ids = [r.get("subject", {}).get("reference", "").replace("Patient/", "") for r in fhir_resources]
        print("\nExtracted Patient IDs:")
        print(list(set(patient_ids)))
        
        logger.info(f"Generated {len(fhir_resources)} resources")
        
        # Step 3: Save raw data
        raw_data_path = os.path.join(output_dir, "raw_fake_fhir_data.json")
        print(f"Saving raw FHIR data to {raw_data_path}...")
        with open(raw_data_path, "w") as f:
            json.dump({"entry": fhir_resources}, f, indent=2)
        logger.info(f"Raw FHIR data saved to {raw_data_path}")
        
        # Step 4: Process data
        preprocessed_ddf = preprocess_fhir_hrv(fhir_resources)
        
        # Print sample of preprocessed data
        print("\nSample of Preprocessed Data:")
        print(preprocessed_ddf.head())
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
