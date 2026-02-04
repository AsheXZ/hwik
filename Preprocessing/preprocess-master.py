"""
Preprocess Kerala environmental CSVs with robust LST filtering and 
Statistical Imputation (Interpolation + Seasonal Medians).
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
import pandas as pd
import numpy as np

# Configuration
EXPECTED_COLUMNS = [
    "grid_id", "date", "year", "month", "lat", "lon",
    "lst_celsius", "ndvi", "ndwi", "radar_vh", "rainfall_mm", "slope", "elevation"
]

# Columns that require statistical imputation
IMPUTE_COLUMNS = ["lst_celsius", "ndvi", "ndwi", "radar_vh", "rainfall_mm"]

# Outlier Thresholds
MIN_VALID_LST, MAX_VALID_LST = 10.0, 60.0
GEE_UNMASK_VALUE = -999.0

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    return df

def clean_and_mask(df: pd.DataFrame) -> dict:
    """Initial cleaning: convert to numeric and mask sentinels/outliers with NaN."""
    mask_stats = {}
    
    for col in IMPUTE_COLUMNS:
        if col not in df.columns: continue
        
        series = pd.to_numeric(df[col], errors="coerce")
        
        # Identify invalid indices
        if col == "lst_celsius":
            invalid = (series == GEE_UNMASK_VALUE) | (series < MIN_VALID_LST) | (series > MAX_VALID_LST)
        else:
            invalid = (series == GEE_UNMASK_VALUE)
            
        mask_stats[col] = int(invalid.sum())
        df[col] = series.mask(invalid)
        
    return mask_stats

def impute_data(df: pd.DataFrame) -> dict:
    """
    Two-stage imputation:
    1. Linear Interpolation per Grid (Time-based)
    2. Seasonal Median Fallback (Month-based)
    """
    impute_counts = {}
    df = df.sort_values(['grid_id', 'date'])

    for col in IMPUTE_COLUMNS:
        if col not in df.columns: continue
        
        initial_nan = df[col].isna().sum()
        
        # Stage 1: Linear Interpolation within each grid
        # Handles missing months if surrounding months exist
        df[col] = df.groupby('grid_id')[col].transform(
            lambda x: x.interpolate(method='linear', limit_direction='both')
        )
        
        # Stage 2: Seasonal Median
        # For grids missing data entirely for a period, fill with the state-wide median for that month
        seasonal_medians = df.groupby('month')[col].transform('median')
        df[col] = df[col].fillna(seasonal_medians)
        
        # If still NaN (rare), fill with global median
        df[col] = df[col].fillna(df[col].median())
        
        impute_counts[col] = int(initial_nan - df[col].isna().sum())

    return df, impute_counts

def preprocess_file(source: Path, output_dir: Path) -> tuple[Path, dict]:
    print(f"--- Processing: {source.name} ---")
    df = pd.read_csv(source)
    df = _normalize_columns(df)
    
    # Standardize Date
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # 1. Masking
    mask_stats = clean_and_mask(df)
    
    # 2. Imputation
    df, impute_stats = impute_data(df)
    
    # Summary Statistics
    stats = {
        "source": source.name,
        "total_rows": len(df),
        "outliers_masked": mask_stats,
        "values_imputed": impute_stats,
        "final_null_count": df[IMPUTE_COLUMNS].isna().sum().to_dict()
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / source.name # Overwrites in the output directory
    df.to_csv(output_path, index=False)
    
    print(f"LST Outliers: {mask_stats.get('lst_celsius', 0)}")
    print(f"Imputed Records: {sum(impute_stats.values())}")
    
    return output_path, stats

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("."))
    parser.add_argument("--output-dir", type=Path, default=Path("./preprocessed"))
    args = parser.parse_args()

    all_stats = []
    csv_files = sorted(list(args.data_dir.glob("kerala_env_*.csv")))

    if not csv_files:
        print("No files found matching kerala_env_*.csv")
        return

    for file_path in csv_files:
        try:
            _, stats = preprocess_file(file_path, args.output_dir)
            all_stats.append(stats)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # Combine all for a final master dataset
    if all_stats:
        combined_df = pd.concat([pd.read_csv(args.output_dir / f.name) for f in csv_files])
        master_path = args.output_dir / "kerala_env_master_imputed.csv"
        combined_df.to_csv(master_path, index=False)
        
        with open(args.output_dir / "imputation_summary.json", "w") as f:
            json.dump(all_stats, f, indent=4)
        
        print(f"\nSUCCESS: Master file created at {master_path}")
        print(f"Summary stats saved to imputation_summary.json")

if __name__ == "__main__":
    main()