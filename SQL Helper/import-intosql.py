import pandas as pd
import geopandas as gpd
import json
from sqlalchemy import create_engine, text
from shapely.geometry import shape
from shapely.wkt import loads
import warnings

# Ignore the specific GeoPandas warning about empty geometries
warnings.filterwarnings('ignore', 'GeoSeries.notna', UserWarning)

# --- CONFIGURATION ---
DB_CONN = "postgresql://ashe:root@localhost:5432/hwik"
ENV_FILES = ["kerala_env_2019.csv", "kerala_env_2020.csv", "kerala_env_2021.csv"]
CONFLICT_FILE = "conflict_locations_geocoded.csv"

engine = create_engine(DB_CONN)

def cleanup_database():
    """Drops existing tables with CASCADE to handle foreign key dependencies."""
    print("\n--- STEP 0: Cleaning up existing database tables ---")
    tables_to_drop = ['conflict_events', 'kerala_env_dynamic', 'kerala_grid_master']
    with engine.connect() as conn:
        for table in tables_to_drop:
            print(f"Dropping table {table} (if exists)...")
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
        conn.commit()

def process_environment_data():
    print("\n--- STEP 1: Processing Environmental Data ---")
    all_env_data = []

    for f in ENV_FILES:
        try:
            print(f"Loading {f}...")
            df = pd.read_csv(f)
            all_env_data.append(df)
        except FileNotFoundError:
            print(f"Error: File {f} not found. Skipping.")

    if not all_env_data:
        print("No environmental data loaded. Exiting.")
        return

    full_df = pd.concat(all_env_data, ignore_index=True)

    print("Parsing Geometries...")
    def parse_geo(geo_str):
        try:
            if pd.isna(geo_str): return None
            return shape(json.loads(geo_str))
        except:
            return None

    full_df['geometry'] = full_df['.geo'].apply(parse_geo)
    gdf_env = gpd.GeoDataFrame(full_df, geometry='geometry', crs="EPSG:4326")

    print("Creating Master Grid & Fixing Geometries...")
    grid_master = gdf_env[['grid_id', 'elevation', 'slope', 'geometry']].drop_duplicates(subset='grid_id')

    def force_polygon(geom):
        if geom is None: return None
        if geom.geom_type in ['Point', 'MultiPoint']:
            return geom.buffer(0.0045).envelope
        return geom

    grid_master['geometry'] = grid_master['geometry'].apply(force_polygon)
    
    # Fixed the warning: Use both notna() and is_empty check
    grid_master = grid_master[grid_master['geometry'].notna() & ~grid_master['geometry'].is_empty]

    grid_master = grid_master.rename(columns={
        'elevation': 'avg_elevation', 
        'slope': 'avg_slope',
        'geometry': 'geom'
    })
    grid_master = grid_master.set_geometry('geom')

    print("Uploading 'kerala_grid_master'...")
    # 'replace' will now work because 'cleanup_database' dropped dependencies
    grid_master.to_postgis('kerala_grid_master', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        print("Restoring Primary Key and Index...")
        conn.execute(text("ALTER TABLE kerala_grid_master ADD PRIMARY KEY (grid_id);"))
        conn.execute(text("CREATE INDEX idx_grid_geom ON kerala_grid_master USING GIST(geom);"))
        conn.commit()

    print(f"Uploaded {len(grid_master)} unique grid cells.")

    print("Creating Time-Series Data...")
    dynamic_df = pd.DataFrame(full_df) 
    cols_to_keep = ['grid_id', 'date', 'year', 'month', 'lst_celsius', 'ndvi', 'ndwi', 'radar_vh', 'rainfall_mm']
    available_cols = [c for c in cols_to_keep if c in dynamic_df.columns]
    dynamic_upload = dynamic_df[available_cols].copy()
    
    if 'date' in dynamic_upload.columns:
        dynamic_upload['obs_date'] = pd.to_datetime(dynamic_upload['date'])
        dynamic_upload = dynamic_upload.drop(columns=['date']) 

    print("Uploading 'kerala_env_dynamic'...")
    dynamic_upload.to_sql('kerala_env_dynamic', engine, if_exists='replace', index=False, method='multi', chunksize=1000)
    
    with engine.connect() as conn:
        print("Restoring Dynamic Table Constraints...")
        conn.execute(text("ALTER TABLE kerala_env_dynamic ADD COLUMN obs_id BIGSERIAL PRIMARY KEY;"))
        conn.execute(text("ALTER TABLE kerala_env_dynamic ADD CONSTRAINT fk_grid FOREIGN KEY (grid_id) REFERENCES kerala_grid_master(grid_id);"))
        conn.execute(text("CREATE INDEX idx_env_date ON kerala_env_dynamic(obs_date);"))
        conn.commit()
        
    print(f"Uploaded {len(dynamic_upload)} observations.")

def process_conflict_data():
    print("\n--- STEP 2: Processing Conflict Data ---")
    try:
        df = pd.read_csv(CONFLICT_FILE)
    except FileNotFoundError:
        print(f"File {CONFLICT_FILE} not found. Skipping.")
        return

    gdf_conflict = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df.long, df.lat),
        crs="EPSG:4326"
    )
    
    print("Linking Conflict Points to Grid IDs...")
    gdf_grid = gpd.read_postgis("SELECT grid_id, geom FROM kerala_grid_master", engine, geom_col='geom')
    
    gdf_joined = gpd.sjoin(gdf_conflict, gdf_grid, how="left", predicate="within")
    gdf_joined = gdf_joined.dropna(subset=['grid_id'])
    
    print(f"Mapped {len(gdf_joined)} events to grids.")

    gdf_joined = gdf_joined.rename(columns={
        'range': 'range_name',
        'place': 'place_name',
        'conflict': 'conflict_type',
        'geometry': 'geom'
    })
    
    final_cols = ['district', 'range_name', 'place_name', 'conflict_type', 'geom', 'grid_id']
    cols_to_upload = [c for c in final_cols if c in gdf_joined.columns]
    gdf_upload = gdf_joined[cols_to_upload]
    gdf_upload = gdf_upload.set_geometry('geom')

    print("Uploading 'conflict_events'...")
    gdf_upload.to_postgis('conflict_events', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        print("Restoring Conflict Table Constraints...")
        conn.execute(text("ALTER TABLE conflict_events ADD COLUMN event_id SERIAL PRIMARY KEY;"))
        conn.execute(text("ALTER TABLE conflict_events ADD CONSTRAINT fk_conflict_grid FOREIGN KEY (grid_id) REFERENCES kerala_grid_master(grid_id);"))
        conn.commit()

    print("Done.")

if __name__ == "__main__":
    try:
        # 1. Clear old data and constraints first
        cleanup_database()
        # 2. Process data
        process_environment_data()
        process_conflict_data()
        print("\nSUCCESS: Database population complete.")
    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")