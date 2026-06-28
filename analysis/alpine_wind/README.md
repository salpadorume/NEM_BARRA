README FOR ANALYSIS/ALPINE_WIND
--------------------------------

This analysis should be run after the notebooks in analysis/wind_chapter.
The core workflow builds alpine and South Australia wind composites, then the study notebooks and plotting notebooks turn those composites into thesis figures.

STAGE 1: BUILD WIND COMPOSITES
------------------------------

SCRIPT 1: bootstrap_composite.ipynb
- Calculates bootstrapped wind vectors for either SA or Alpine NSW on heatwave and baseline days.
- This is the main composite-building notebook for the alpine wind chapter.

SCRIPT 2: calc_composite.ipynb
- Builds the same composites without bootstrap resampling.
- Useful for direct comparison with the bootstrapped results.

SCRIPT 3: R2_calc_composite.ipynb
- Tests the R2 versus C2 composite setup.
- Use this notebook when checking whether the chosen region/composite definition is behaving as expected.

SCRIPT 4: variance.ipynb
- Calculates the variance between heatwave and baseline composites.
- Provides a compact summary of how much the two regimes differ.

STAGE 2: STUDY AND PRESENTATION NOTEBOOKS
-----------------------------------------

SCRIPT 1: alpine_study.ipynb
- Main alpine wind case study notebook.
- Produces the timeseries figures and summaries used for the Alpine NSW case study.

SCRIPT 2: sa_study.ipynb
- South Australia counterpart to alpine_study.ipynb.
- Uses the same composite logic to show the SA wind response.

SCRIPT 3: plot_bootstrapped.ipynb
- Plots the bootstrapped composite outputs for Alpine NSW.
- Mostly presentation code rather than core processing.

SCRIPT 4: sa_plot_bootstrapped.ipynb
- South Australia plotting counterpart to plot_bootstrapped.ipynb.
- Use this when generating the SA figures for the chapter.
