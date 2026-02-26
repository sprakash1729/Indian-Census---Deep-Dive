import sqlite3
import pandas as pd

# Path to our SQLite database
db_path = '../data/processed/census_model.db'

def run_test_query():
    print("🔌 Connecting to the database...\n")
    
    # Create the connection
    conn = sqlite3.connect(db_path)
    
    # Write our SQL query using a JOIN
    # We are calculating the urbanization percentage on the fly to test the math
    query = """
    SELECT 
        d.state_name,
        d.district_name,
        f.population,
        f.urban_population
    FROM fact_census f
    JOIN dim_location d ON f.location_id = d.location_id
    WHERE f.population > 0 
    ORDER BY f.urban_population DESC
    LIMIT 10;
    """
    
    try:
        # Execute the query and load results into a Pandas DataFrame for clean printing
        results_df = pd.read_sql_query(query, conn)
        
        print("🏆 Top 10 Districts by Urban Population:\n")
        # Print the dataframe without the index column for a cleaner look
        print(results_df.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error running query: {e}")
        print("Double-check that the column names in the SELECT statement match your raw CSV exactly.")
        
    finally:
        # Always close the connection!
        conn.close()

if __name__ == "__main__":
    run_test_query()