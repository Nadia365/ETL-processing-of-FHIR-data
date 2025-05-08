import os 
import pandas as pd


def visualize_biosignals_interactive(
    observations: List[Dict[str, Any]], output_dir: str = "fhir_analysis"
) -> None:
    """
    Visualize biosignals (e.g., HRV) as interactive time-series plots using Plotly and Dask.

    Args:
        observations: List of observation dictionaries.
        output_dir: Directory to save HTML plot files.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to Dask DataFrame for scalability
    df = pd.DataFrame(observations)
    if df.empty:
        logger.warning("No observations available for visualization")
        return
    
    ddf = dd.from_pandas(df, npartitions=2)
    ddf["timestamp"] = dd.to_datetime(ddf["timestamp"], utc=True)
    
    # Compute unique patients
    patient_ids = ddf["patient_id"].unique().compute()
    
    for patient_id in patient_ids:
        # Filter data for the patient
        patient_ddf = ddf[ddf["patient_id"] == patient_id].persist()
        
        # Compute unique codes for this patient
        codes = patient_ddf["code"].unique().compute()
        if codes.empty:
            logger.info(f"No biosignals for patient {patient_id}")
            continue
        
        # Create a Plotly figure
        fig = go.Figure()
        
        for code in codes:
            # Filter by code and compute
            code_ddf = patient_ddf[patient_ddf["code"] == code][["timestamp", "value", "unit"]]
            code_df = code_ddf.compute()  # Convert to Pandas for plotting
            if code_df.empty:
                logger.info(f"No data for {code} for patient {patient_id}")
                continue
                
            # Sort by timestamp for proper time-series
            code_df = code_df.sort_values("timestamp")
            
            # Add trace for this biosignal
            unit = code_df["unit"].iloc[0] if not code_df["unit"].empty else ""
            display_name = "HRV" if code == "80404-7" else code  # Human-readable for HRV
            fig.add_trace(
                go.Scatter(
                    x=code_df["timestamp"],
                    y=code_df["value"],
                    mode="lines+markers",
                    name=f"{display_name} ({unit})",
                    hovertemplate=f"{display_name}: %{{y}} {unit}<br>Time: %{{x|%Y-%m-%d %H:%M:%S}}<extra></extra>"
                )
            )
        
        # Update layout
        fig.update_layout(
            title=f"Biosignals for Patient {patient_id}",
            xaxis_title="Time",
            yaxis_title="Value",
            hovermode="x unified",
