# NEM_BARRA

STAGE 1: RETRIEVE NEM DATA.
---------------------------

The MMSDM DISPATCHLOAD tables were downloaded from https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/ (retrieved November 2025, prior to the Data Model redesign in December).
Location: saved to my folder in project ng72 on Gadi.

Generator details were retrieved from the NEM Generation Information spreadsheets from June 2019 to July 2024. Generation Information prior to June 2019 did not contain Dispatchable Unit Identifiers (DUID), which are needed to identify generators in the dispatch tables.
The generator details were concatenated and the most recent entry for each DUID was selected, this may mean that details such as regular and maximum capacity may not apply to all periods of generation.
Link: https://www.aemo.com.au/energy-systems/electricity/national-electricity-market-nem/nem-forecasting-and-planning/forecasting-and-planning-data/generation-information (retrieved July 2024).
Location: ID_HW_BARRA/data/raw/gen_info.csv

Some generator locations were retrieved from NationalMap (accessed June 2025, now retired). 
Location: ID_HW_BARRA/data/raw/nmap.csv

STAGE 2: IDENTIFY HEATWAVES IN BARRA-R2
---------------------------------------

SCRIPT 1: calc_ls_mask.ipynb
- Reads the sftlf (land_area_fraction) from AUS-11 (BARRA-R2) in project ob53.
- Masks land fraction <= 0.75 and saves as NetCDF.

Reads: "/g/data/ob53/BARRA2/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/fx/sftlf/latest/sftlf_AUS-11_ERA5_historical_hres_BOM_BARRA-R2_v1.nc"
Writes: "ID_HW_BARRA/data/preprocess/land_sea_mask.nc"
Compute: Medium (4CPU, 18GB) should be fine as the file is small.
Environment: analysis3

SCRIPT 2: calc95pct.ipynb
- Calculates mean daily temperature (tas) climatology for 1979-2000 using AUS-11 (BARRA-R2).

Reads: "/g/data/ob53/BARRA2/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/day/tas/latest/"
Writes: "ID_HW_BARRA/data/preprocess/t95_baseline.nc"
Compute: xxlarge (28CPU, 126GB)
Environment: analysis3

SCRIPT 3: calcehf.ipynb
- Calculates daily EHF for land cells in BARRA-R2.
- D'Argueso's EHF code using Nairn and Fawcett (2014) definition of EHF.
- Parrallelized using Dask and Xarray.

Reads: "/g/data/ob53/BARRA2/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/day/tas/latest/"
Writes: '/scratch/ng72/ms5578/ehf_netcdf/'
Environment: analysis3
Compute: xxlarge

STAGE 3: PROCESS NEM DATA
-------------------------

SCRIPT 1: get_coordinates.ipynb
- Merges latitude and longitude coordinates from NationalMap with the Generation Information (master list).
- Fills remaining gaps with Google Maps' Find Place API.
- Remaining locations filled manually, omitted where locations could not be found.
Reads: "ID_HW_BARRA/data/raw/nmap.csv", "ID_HW_BARRA/data/raw/gen_info.csv"
Writes: '/ID_HW_BARRA/data/preprocess/gen_info.csv'
Environment: custom (also requires API setup)
Compute: medium

SCRIPT 2: 