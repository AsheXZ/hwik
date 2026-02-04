import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from shapely.geometry import Point

# --- CONFIGURATION ---
DB_CONN = "postgresql://ashe:root@localhost:5432/hwik"
CSV_FILE = "kerala_env_master_imputed.csv"
engine = create_engine(DB_CONN)

# Approximate 2km grid side in degrees
GRID_SIDE = 0.018 

def process_and_append():
    print(f"Reading {CSV_FILE}...")
    df = pd.read_csv(CSV_FILE)

    # --- 1. PREPARE KERALA GRID MASTER ---
    # We extract unique grid IDs and create square polygons from lat/lon
    print("Preparing Master Grid (Polygons)...")
    grid_df = df[['grid_id', 'elevation', 'slope', 'lat', 'lon']].drop_duplicates(subset='grid_id').copy()
    
    # Create the geometry GeoSeries
    # cap_style=3 creates a square buffer around the point
    geometry = [Point(xy).buffer(GRID_SIDE/2, cap_style=3) for xy in zip(grid_df.lon, grid_df.lat)]
    
    # Initialize GeoDataFrame
    gdf_grid = gpd.GeoDataFrame(grid_df, geometry=geometry, crs="EPSG:4326")
    
    # Rename columns to match your schema
    gdf_grid = gdf_grid.rename(columns={
        'elevation': 'avg_elevation',
        'slope': 'avg_slope',
        'geometry': 'geom' 
    })
    
    # --- ACTIVE GEOMETRY SET ---
    # Explicitly setting 'geom' as the active geometry column as requested
    gdf_grid = gdf_grid.set_geometry("geom")
    
    # Select only relevant columns
    grid_upload = gdf_grid[['grid_id', 'avg_elevation', 'avg_slope', 'geom']]

    # --- 2. PREPARE KERALA ENV DYNAMIC ---
    print("Preparing Environmental Time-Series...")
    dynamic_upload = df.rename(columns={
        'date': 'obs_date',
        'lst_celsius': 'lst_celsius',
        'ndvi': 'ndvi',
        'ndwi': 'ndwi',
        'radar_vh': 'radar_vh',
        'rainfall_mm': 'rainfall_mm'
    })
    
    # Ensure obs_date is a datetime object
    dynamic_upload['obs_date'] = pd.to_datetime(dynamic_upload['obs_date'])
    
    # We do NOT include 'obs_id' so that the BIGSERIAL auto-increments in Postgres
    dynamic_cols = [
        'grid_id', 'obs_date', 'year', 'month', 
        'lst_celsius', 'ndvi', 'ndwi', 'radar_vh', 'rainfall_mm'
    ]
    dynamic_upload = dynamic_upload[dynamic_cols]

    # --- 3. EXECUTE DATABASE APPEND ---
    with engine.connect() as conn:
        # Step A: Append Grid Master
        # Using a temporary table to handle "ON CONFLICT" logic 
        # (This avoids errors if grid_ids already exist)
        print("Uploading Grid Master...")
        grid_upload.to_postgis("temp_grid_upload", engine, if_exists="replace", index=False)
        
        conn.execute(text("""
            INSERT INTO kerala_grid_master (grid_id, avg_elevation, avg_slope, geom)
            SELECT grid_id, avg_elevation, avg_slope, geom FROM temp_grid_upload
            ON CONFLICT (grid_id) DO NOTHING;
        """))
        conn.execute(text("DROP TABLE temp_grid_upload;"))
        conn.commit()

        # Step B: Append Dynamic Data
        print("Uploading Dynamic Time-Series...")
        dynamic_upload.to_sql(
            'kerala_env_dynamic', 
            engine, 
            if_exists='append', 
            index=False, 
            method='multi', 
            chunksize=1000
        )
        conn.commit()

    print("\nSUCCESS: Data appended to 'hwik' database.")

if __name__ == "__main__":
    try:
        process_and_append()
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")