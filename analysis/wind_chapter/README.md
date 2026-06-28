README FOR ANALYSIS/WIND_CHAPTER
--------------------------------

This chapter should be run before the notebooks in analysis/alpine_wind. The main workflow builds wind-profile difference curves, clusters the wind units, and then uses those cluster maps in the bootstrap and summary notebooks.

STAGE 1: BUILD WIND PROFILE CURVES
----------------------------------

SCRIPT 1: block_bootstrap_diff.ipynb
- Bootstraps the wind generation profiles and calculates the difference curves used in later clustering notebooks.
- Also appends generator metadata and cluster labels where available.  
Reads: "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv", generation time-series inputs, "ID_HW_BARRA/data/preprocess/gen_info.csv"  
Writes: "ID_HW_BARRA/data/preprocess/wind_difference_curves.csv"  
Environment: analysis3  
Compute: xxlarge  

SCRIPT 2: timeseries_clustering.ipynb
- Clusters the wind difference curves into profile groups.
- Uses generator coordinates to map the resulting clusters back to the DUID locations.  
Reads: "ID_HW_BARRA/data/preprocess/wind_difference_curves.csv", "ID_HW_BARRA/data/preprocess/gen_info.csv"  
Writes: "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv"  
Environment: tseries + conda/analysis3 (custom)  
Compute: large  

SCRIPT 3: kmeans.ipynb
- Runs spatial clustering for the wind generators and evaluates cluster quality.
- Saves the spatial cluster map and silhouette table for later notebooks.  
Reads: "ID_HW_BARRA/data/preprocess/gen_info.csv", cluster inputs used in the notebook  
Writes: "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv", "ID_HW_BARRA/data/output/wind_chapter/silhouette_table.csv"  
Environment: analysis3  
Compute: large  

STAGE 2: WIND SUMMARY AND DIAGNOSTIC NOTEBOOKS
----------------------------------------------

SCRIPT 1: wind_details.ipynb
- Builds the wind-location and wind-summary tables used in the chapter text and figures.
- Combines the spatial and profile cluster maps with generator metadata.  
Reads: "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv", "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv"  
Writes: "ID_HW_BARRA/data/output/wind_chapter/wind_details_table.csv", "ID_HW_BARRA/data/output/wind_chapter/wind_loc_table.csv"  
Environment: analysis3  
Compute: large  

SCRIPT 2: block_bootstrap_cluster.ipynb
- Bootstraps wind data by spatial cluster.
- Uses the spatial cluster map produced by kmeans.ipynb.  
Reads: "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv"  
Writes: derived tables and figures in data/output/wind_chapter  
Environment: analysis3  
Compute: xxlarge  

SCRIPT 3: block_bootstrap_DUID.ipynb
- Bootstraps wind data by profile cluster and DUID.
- Uses the profile cluster map produced by timeseries_clustering.ipynb.  
Reads: "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv"  
Writes: "ID_HW_BARRA/data/preprocess/DUID_curves.csv" and chapter figures  
Environment: analysis3  
Compute: xxlarge  

SCRIPT 4: block_bootstrap_region.ipynb
- Bootstraps wind data by region.
Reads: wind chapter generation inputs and cluster metadata  
Writes: derived tables and figures in data/output/wind_chapter  
Environment: analysis3  
Compute: xxlarge  

SCRIPT 5: hw_day_block_bootstrap.ipynb
- Bootstraps wind data by heatwave-day grouping.
- Uses the wind spatial cluster map produced in kmeans.ipynb to compare heatwave-day regimes.  
Reads: "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv"  
Writes: derived tables and figures in data/output/wind_chapter  
Environment: analysis3  
Compute: xxlarge  

