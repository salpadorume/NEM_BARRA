README FOR ANALYSIS/ALPINE_WIND
--------------------------------

This analysis should be run after the notebooks in analysis/wind_chapter.
The core workflow builds alpine and South Australia wind composites, then the study notebooks and plotting notebooks turn those composites into thesis figures.

STAGE 1: BUILD WIND COMPOSITES
------------------------------

SCRIPT 1: bootstrap_composite.ipynb
- Calculates bootstrapped wind vectors for either SA or Alpine NSW on heatwave and baseline days.
- This is the main composite-building notebook for the alpine wind chapter.  
Reads: "ID_HW_BARRA/data/preprocess/gen_info.csv", "ID_HW_BARRA/data/preprocess/hw_{area}_cluster.csv", "ID_HW_BARRA/data/preprocess/no_hw_{area}_cluster.csv", BARRA-C2 wind data from /g/data/ob53/  
Writes: "ID_HW_BARRA/data/preprocess/{area}_bootstrap_summary.nc"  
Environment: analysis3  
Compute: xxlarge

SCRIPT 2: calc_composite.ipynb
- Builds the same composites without bootstrap resampling.
- Useful for direct comparison with the bootstrapped results.  
Reads: "ID_HW_BARRA/data/preprocess/gen_info.csv", "ID_HW_BARRA/data/preprocess/no_hw_alpine_cluster.csv", BARRA-C2 wind data  
Writes: "ID_HW_BARRA/data/preprocess/alpine_BARRA-C2_baseline_composites.nc"  
Environment: analysis3  
Compute: xxlarge

SCRIPT 3: R2_calc_composite.ipynb
- Tests the R2 versus C2 composite setup.
- Use this notebook when checking whether the chosen region/composite definition is behaving as expected.  
Reads: "ID_HW_BARRA/data/preprocess/gen_info.csv", cluster CSV files, BARRA-C2/R2 wind data, shapefiles  
Writes: PNG plots to "ID_HW_BARRA/data/output/alpine_wind/"  
Environment: analysis3  
Compute: large

SCRIPT 4: variance.ipynb
- Calculates the variance between heatwave and baseline composites.
- Provides a compact summary of how much the two regimes differ.  
Reads: "ID_HW_BARRA/data/preprocess/gen_info.csv", cluster CSV files, BARRA-C2 wind data  
Writes: "{area}_mean_variance_summary.nc", PNG plots to "ID_HW_BARRA/data/output/alpine_wind/"  
Environment: analysis3  
Compute: xxlarge

STAGE 2: STUDY AND PRESENTATION NOTEBOOKS
-----------------------------------------

SCRIPT 1: alpine_study.ipynb
- Main alpine wind case study notebook.
- Produces the timeseries figures and summaries used for the Alpine NSW case study.  
Reads: "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv", "ID_HW_BARRA/data/preprocess/DUID_curves.csv", "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv", "ID_HW_BARRA/data/preprocess/wind_difference_curves.csv"  
Writes: Inline plots (no explicit file writes)  
Environment: analysis3  
Compute: medium 

SCRIPT 2: sa_study.ipynb
- South Australia counterpart to alpine_study.ipynb.
- Uses the same composite logic to show the SA wind response.  
Reads: "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv", "ID_HW_BARRA/data/preprocess/DUID_curves.csv", "ID_HW_BARRA/data/preprocess/wind_spatial_clusters.csv", "ID_HW_BARRA/data/preprocess/wind_difference_curves.csv"  
Writes: Inline plots (no explicit file writes)  
Environment: analysis3  
Compute: medium

SCRIPT 3: plot_bootstrapped.ipynb
- Plots the bootstrapped composite outputs for Alpine NSW.
- Mostly presentation code rather than core processing.  
Reads: "ID_HW_BARRA/data/preprocess/alpine_bootstrap_summary.nc", "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv", "ID_HW_BARRA/data/preprocess/hw_alpine_cluster.csv", shapefiles  
Writes: PNG plots to "ID_HW_BARRA/data/output/alpine_wind/"  
Environment: analysis3
Compute: large 

SCRIPT 4: sa_plot_bootstrapped.ipynb
- South Australia plotting counterpart to plot_bootstrapped.ipynb.
- Use this when generating the SA figures for the chapter.  
Reads: "ID_HW_BARRA/data/preprocess/sa_bootstrap_summary.nc", "ID_HW_BARRA/data/preprocess/wind_profile_clusters.csv", "ID_HW_BARRA/data/preprocess/hw_sa_cluster.csv", shapefiles  
Writes: PNG plots to "ID_HW_BARRA/data/output/alpine_wind/"  
Environment: analysis3  
Compute: large
