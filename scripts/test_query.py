import sqlite3
import pandas as pd

# Path to our SQLite database
db_path = '../data/processed/census_model.db'

def run_test_query():
    print("🔌 Connecting to the database...\n")
    
    conn = sqlite3.connect(db_path)
    
    # Updated SQL query using the actual columns: 
    # urban_households and households
    query = """
    SELECT 
        d.state_name,
        d.district_name,
        f.population,
        f.households,
        f.urban_households,
        ROUND((CAST(f.urban_households AS FLOAT) / f.households) * 100, 2) AS urban_household_pct
    FROM fact_census f
    JOIN dim_location d ON f.location_id = d.location_id
    WHERE f.households > 0 
    ORDER BY urban_household_pct DESC
    LIMIT 10;
    """
    
    try:
        results_df = pd.read_sql_query(query, conn)
        print("🏆 Top 10 Districts by Urban Household Percentage:\n")
        print(results_df.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error running query: {e}")
        
    finally:
        conn.close()

if __name__ == "__main__":
    run_test_query()