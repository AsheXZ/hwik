# Kerala Human–Wildlife Conflict Risk Analysis

A geospatial data engineering and analytics pipeline for modeling **human–wildlife conflict (HWC)** in Kerala using **hexagonal grids (H3)**, **remote sensing**, **citizen science**, and **news-derived events**, backed by **PostGIS**.

---

## 📌 Project Overview

This project builds a **grid-based spatio-temporal dataset** that integrates:

- Static terrain features (elevation, slope)
- Dynamic environmental variables (LST, NDVI, NDWI, rainfall, radar)
- Human–wildlife conflict events from multiple sources
- Validation signals from citizen science (GBIF) and clustering analysis

The system is designed to support **risk modeling, hotspot detection, and future simulation**.

---

**Important: All earth engine scripts are found at** https://code.earthengine.google.com/57ae06f0ffa5085a271e235ec641ad08

## 🗺️ Validation Architecture

![Validation](validation.png)

*Web scraped data using various methods used to cluster validation hotspots using KMeans*
The boundaries are define from admin shape files provided in the respective folder.

---

## 🔧 Pipeline Components

### 1. Spatial Grid Generation (H3 + Google Earth Engine)
**File:** `h3-earthengine-collect.py`

- Generates an H3 hexagonal grid over Kerala
- Robust bounding-box strategy with geometry filtering
- Verifies Earth Engine extraction (elevation)
- Designed for resolution scaling (H3 res 6 → 8)

---

### 2. Environmental Data Ingestion → PostGIS
**File:** `import-intosql.py`

- Cleans and rebuilds database schema
- Creates:
  - `kerala_grid_master` (static polygons)
  - `kerala_env_dynamic` (time-series features)
  - `conflict_events` (point-based events)
- Handles geometry repair, indexing, and FK constraints
- Optimized for large CSV ingestion

---

### 3. Conflict Event Mining (News + Citizen Media)
**File:** `webscraper-nomatim-english.py`

- Scrapes:
  - GDELT (news/blogs)
  - YouTube (citizen reports)
- NLP-based location extraction (spaCy)
- Rate-limited geocoding with caching (Nominatim)
- Species inference + H3 indexing
- Outputs geocoded conflict candidates

---

### 4. Citizen Science Validation (GBIF)
**File:** `pdf-miner.py`

- Queries GBIF/iNaturalist for key species:
  - Elephant, Tiger, Wild Boar, Gaur
- Filters for **human observations**
- Prepares data for land-cover validation
- Intended for conflict proxy validation

---

### 5. Spatial Clustering & Visualization
**File:** `visualize-validation-data.py`

- K-Means clustering of conflict points
- Visualizes:
  - Raw clusters
  - Cluster centroids
  - Overlay on Kerala state & district boundaries
- Used for hotspot sanity checks

---

## 📊 Current Progress

### ✅ Completed
- H3 grid generation over Kerala
- PostGIS schema with spatial indexing
- Environmental data ingestion (multi-year)
- Conflict event table with grid linkage
- News + YouTube mining with NLP geocoding
- GBIF-based validation dataset
- Cluster-based exploratory analysis

### 🟡 In Progress
- Land-cover overlay (ESA WorldCover) for validation
- Temporal aggregation (monthly / seasonal risk)
- Feature normalization & lagged variables
- Conflict label confidence scoring

### 🔜 Planned
- Risk modeling (grid × time)
- Bayesian / ML-based hotspot prediction
- Simulation-ready data export
- Interactive map dashboard (PostGIS + frontend)

---

## 🧠 Design Philosophy

- **Grid-first**: avoids administrative bias
- **Multi-source**: reduces single-source noise
- **Explainable**: spatial + temporal grounding
- **Scalable**: resolution-agnostic (H3)

---

## 📂 Repository Structure (Key Files)

```text
├── h3-earthengine-collect.py
├── import-intosql.py
├── webscraper-nomatim-english.py
├── pdf-miner.py
├── visualize-validation-data.py
├── schema.png
└── README.md
