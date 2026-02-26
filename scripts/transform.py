import pandas as pd
import os

# 1. Define our file paths
input_file = '../data/raw/india-districts-census-2011.csv'
output_dir = '../data/processed/'
output_file = output_dir + 'cleaned_census_2011.parquet'

def run_pipeline():
    print("🚀 Starting Data Transformation...")
    
    # Load the raw CSV
    try:
        df = pd.read_csv(input_file)
        print(f"✅ Loaded raw data: {df.shape[0]} rows and {df.shape[1]} columns.")
    except FileNotFoundError:
        print("❌ Error: Could not find the raw data file. Check your paths!")
        return

    # Standardize column names (lowercase, replace spaces/hyphens with underscores)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
    
    # Feature Engineering: Calculate Urbanization Percentage
    # Note: Adjust column names if the raw CSV uses slightly different headers
    if 'population' in df.columns and 'urban_population' in df.columns:
        df['urbanization_pct'] = round((df['urban_population'] / df['population']) * 100, 2)
        print("✅ Engineered new feature: urbanization_pct")

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save to Parquet format (Requires pyarrow or fastparquet)
    # Parquet is highly compressed and perfect for Power BI ingestion
    df.to_parquet(output_file, engine='pyarrow', index=False)
    print(f"✅ Transformation complete! Saved to {output_file}")

if __name__ == "__main__":
    run_pipeline()