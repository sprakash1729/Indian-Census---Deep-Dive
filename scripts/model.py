import pandas as pd
import sqlite3
import os

processed_file = '../data/processed/cleaned_census_2011.parquet'
db_path = '../data/processed/census_model.db'

def build_data_model():
    print("🏗️ Starting Data Modeling...")
    
    # 1. Load the clean flat file
    df = pd.read_parquet(processed_file)
    
    # 2. Create the Dimension Table (dim_location)
    # Assuming columns 'state_name' and 'district_name' exist. 
    # Adjust names if your CSV used slightly different headers!
    dim_location = df[['state_name', 'district_name']].drop_duplicates().reset_index(drop=True)
    dim_location.insert(0, 'location_id', range(1, 1 + len(dim_location)))
    
    # 3. Create the Fact Table (fact_census)
    # Merge the location_id back into the main dataframe
    fact_census = pd.merge(df, dim_location, on=['state_name', 'district_name'])
    
    # Drop the text columns from the fact table, keeping only the ID and the numbers
    cols_to_drop = ['state_name', 'district_name']
    fact_census = fact_census.drop(columns=cols_to_drop)
    
    # 4. Load into SQLite Database
    print("💾 Saving to SQLite database...")
    conn = sqlite3.connect(db_path)
    
    # Write tables to the database
    dim_location.to_sql('dim_location', conn, if_exists='replace', index=False)
    fact_census.to_sql('fact_census', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✅ Data model successfully built and saved to {db_path}!")

if __name__ == "__main__":
    build_data_model()