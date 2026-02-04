import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text

# --- CONFIGURATION ---
DB_CONN = "postgresql://ashe:root@localhost:5432/hwik"
CONFLICT_CSV = "conflict_locations_geocoded.csv"

engine = create_engine(DB_CONN)

def upload_conflict_events():
    print(f"Loading {CONFLICT_CSV}...")
    df = pd.read_csv(CONFLICT_CSV)

    # 1. Convert CSV to GeoDataFrame (Points)
    # We use long/lat from your CSV headers
    gdf_conflict = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df['long'], df['lat']),
        crs="EPSG:4326"
    )

    # 2. Fetch the Grid Master from the database
    # We need this to find out which grid_id belongs to each lat/long
    print("Fetching grid polygons from database for spatial join...")
    gdf_grid = gpd.read_postgis(
        "SELECT grid_id, geom FROM kerala_grid_master", 
        engine, 
        geom_col='geom'
    )

    # 3. Spatial Join: Link Points to Polygons
    # This finds which grid_id each conflict point falls 'within'
    print("Performing spatial join (linking points to grid_ids)...")
    gdf_joined = gpd.sjoin(gdf_conflict, gdf_grid, how="left", predicate="within")

    # 4. Prepare for Schema match
    # Your schema: district, range_name, place_name, conflict_type, geom, grid_id
    gdf_upload = gdf_joined.rename(columns={
        'range': 'range_name',
        'place': 'place_name',
        'conflict': 'conflict_type',
        'geometry': 'geom'
    })

    # Select only the columns defined in your database schema
    # event_id is SERIAL, so we omit it
    final_cols = ['district', 'range_name', 'place_name', 'conflict_type', 'geom', 'grid_id']
    gdf_upload = gdf_upload[final_cols]

    # Handle cases where a point might fall outside your grids (null grid_id)
    null_count = gdf_upload['grid_id'].isna().sum()
    if null_count > 0:
        print(f"Warning: {null_count} points fell outside any known grid and will be dropped.")
        gdf_upload = gdf_upload.dropna(subset=['grid_id'])

    # Set active geometry
    gdf_upload = gdf_upload.set_geometry("geom")

    # 5. Append to Database
    print(f"Appending {len(gdf_upload)} events to 'conflict_events'...")
    gdf_upload.to_postgis(
        "conflict_events", 
        engine, 
        if_exists="append", 
        index=False
    )

    print("SUCCESS: Conflict events uploaded and linked to grids.")

if __name__ == "__main__":
    try:
        upload_conflict_events()
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")