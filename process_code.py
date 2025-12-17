import os
import datetime
import time
import pandas as pd
import numpy as np
import dask.dataframe as dd
import psutil
import logging

# ----------------------
# Constants for filepaths
# ----------------------
GEN_INFO_FP = "/g/data/ng72/ms5578/ID_HW_BARRA/data/preprocess/gen_info.csv"
HW_FP = "/scratch/ng72/ms5578/gen_hw_status.csv"
GEN_FPATH = "/scratch/ng72/ms5578/generation_time_series"

# ----------------------
# Dask client singleton (auto-starts on import)
# ----------------------
_dask_client = None

def get_dask_client(
    workload_type="io",
    max_workers=None,
    reserve_mem_gb=50,
    max_mem_gb=None,
    dashboard=True,
    dashboard_address=":8787"
):
    global _dask_client
    if _dask_client is not None:
        print(f"Dask dashboard: {_dask_client.dashboard_link}")
        return _dask_client

    from dask.distributed import Client
    logical_cores = psutil.cpu_count(logical=True)
    total_memory_gb = psutil.virtual_memory().total / 1e9
    if max_workers is None:
        max_workers = logical_cores
    if max_mem_gb is None:
        max_mem_gb = total_memory_gb
    usable_mem_gb = max_mem_gb - reserve_mem_gb
    if workload_type == "cpu":
        threads_per_worker = 1
        n_workers = min(max_workers, logical_cores)
    elif workload_type == "io":
        threads_per_worker = 8
        n_workers = max(1, logical_cores // threads_per_worker)
    else:  # "mixed"
        threads_per_worker = 4
        n_workers = max(1, logical_cores // threads_per_worker)
    memory_per_worker = usable_mem_gb // n_workers
    logging.getLogger("distributed.worker.memory").setLevel(logging.ERROR)
    logging.getLogger("dask").setLevel(logging.ERROR)
    _dask_client = Client(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        memory_limit=f"{int(memory_per_worker)}GB",
        dashboard_address=dashboard_address if dashboard else None
    )
    print(f"Dask dashboard: {_dask_client.dashboard_link}")
    return _dask_client

get_dask_client(dashboard=True, dashboard_address=":8787")

# ----------------------
# Selection helpers
# ----------------------
def select_group(gen_details_dd, state=None, ftype=None):
    """Selects a group from the Dask DataFrame."""
    grp_dd = gen_details_dd
    if state is not None:
        grp_dd = grp_dd[grp_dd['region'] == state]
    
    if ftype is not None:
        if isinstance(ftype, list):
            grp_dd = grp_dd[grp_dd['fuel_source_primary'].isin(ftype)]
        else:
            grp_dd = grp_dd[grp_dd['fuel_source_primary'] == ftype]
            
    return grp_dd

# ----------------------
# Processing
# ----------------------
def process_group_dask(grp_pd, grp_dd, gen_fpath, hw_tseries_dd, start_date, end_date, mode="daily"):
    timings = {}
    print("--- Starting Dask-Native Process ---")

    # 1. Get safe DUIDs
    t0 = time.time()
    safe_duids = set(duid.replace("/", "_").replace("\\", "_") for duid in grp_pd['DUID'])
    timings['duid_setup'] = time.time() - t0

    # 2. Read generation data
    t0 = time.time()
    dfs =  dd.read_csv( f"{gen_fpath}/*.csv", dtype={ 'DUID': 'object', 'AGCSTATUS': 'object', 'TOTALCLEARED': 'object', 'TOTALMWh': 'object', }, assume_missing=True, blocksize="256MB", na_values=["", "NA", "NaN"] )
    timings['csv_bulk_read'] = time.time() - t0

    # 3. Filter DUIDs
    t0 = time.time()
    dfs = dfs[dfs['DUID'].isin(safe_duids)]
    dfs = dfs[dfs['DUID'] != 'DUID']
    timings['filter_and_clean'] = time.time() - t0

    # 4. Type conversions
    t0 = time.time()
    dfs['time'] = dd.to_datetime(dfs['time'], errors='coerce', utc=True)
    dfs['TOTALCLEARED'] = dd.to_numeric(dfs['TOTALCLEARED'], errors='coerce')
    dfs['TOTALMWh'] = dd.to_numeric(dfs['TOTALMWh'], errors='coerce')
    dfs['AGCSTATUS'] = dd.to_numeric(dfs['AGCSTATUS'], errors='coerce').fillna(0).astype(int)
    dfs = dfs.dropna(subset=['time', 'DUID'])
    timings['type_conversion_and_dropna'] = time.time() - t0

    # 5. Filter by date
    t0 = time.time()
    dfs = dfs[(dfs['time'] >= start_date) & (dfs['time'] <= end_date)]
    timings['date_filter'] = time.time() - t0

    # 6. Prepare heatwave time series
    t0 = time.time()
    hw_tseries_dd['time'] = dd.to_datetime(hw_tseries_dd['time'], errors='coerce', utc=True)
    hw_tseries_dd['time'] = hw_tseries_dd['time'].dt.normalize()  # daily flag
    hw_tseries_dd = hw_tseries_dd.set_index('time').loc[start_date:end_date].reset_index()
    timings['hw_tseries_filter'] = time.time() - t0

    # 7. Merge and aggregate
    t0 = time.time()
    if mode == 'hourly':
        dfs['merge_day'] = dfs['time'].dt.normalize()
        hw_tseries_dd['merge_day'] = hw_tseries_dd['time']
        merged = dd.merge(
            dfs,
            hw_tseries_dd,
            on=['DUID', 'merge_day'],
            how='left'
        )
        merged = merged.drop(columns=['merge_day'])
        # Fix: Ensure 'time' is present
        if 'time_x' in merged.columns:
            merged = merged.rename(columns={'time_x': 'time'})
        if 'time_y' in merged.columns:
            merged = merged.drop(columns=['time_y'])
        timings['aggregate_hourly'] = time.time() - t0
    else:  # daily mode
        # For daily, aggregate and merge on normalized time
        dfs['time'] = dfs['time'].dt.normalize()
        merged = dd.merge(
            dfs,
            hw_tseries_dd,
            on=['DUID', 'time'],
            how='left'
        )
        agg_func = {
            'TOTALMWh': 'sum',
            'TOTALCLEARED': 'sum',
            'AGCSTATUS': 'max',
            'EHF_flag': 'max'
        }
        merged = merged.groupby(['DUID', 'time']).agg(agg_func).reset_index()
        timings['aggregate_daily'] = time.time() - t0

    # 8. Drop all-NA rows
    merged = merged.dropna(how='all')

    # 9. Jitter for plotting
    t0 = time.time()
    grp_pd_jittered = grp_pd.groupby(['lat', 'lon'], group_keys=False).apply(
        lambda g: g.assign(
            lat_jittered=g['lat'] + np.random.uniform(-0.05, 0.05, size=len(g)) * np.arange(len(g)),
            lon_jittered=g['lon'] + np.random.uniform(-0.05, 0.05, size=len(g)) * np.arange(len(g))
        )
    )
    timings['jitter'] = time.time() - t0

    # 10. Final merge with generator details
    t0 = time.time()
    grp_pd_jittered_dd = dd.from_pandas(grp_pd_jittered, npartitions=1)
    merged = merged.merge(
        grp_pd_jittered_dd[['DUID', 'fuel_source_primary', 'region', 'lat_jittered', 'lon_jittered']],
        on='DUID',
        how='left'
    )
    timings['final_merge'] = time.time() - t0

    # 11. Compute the final result
    print("Starting final Dask compute...")
    t0 = time.time()
    df_computed = merged.compute()
    timings['compute'] = time.time() - t0
    print("Dask compute finished.")

    print("\n--- DASK TIMING REPORT ---")
    for k, v in timings.items():
        print(f"{k}: {v:.2f} seconds")
    print("--- END REPORT ---\n")

    return df_computed.reset_index(drop=True), grp_pd_jittered.reset_index(drop=True)

# ----------------------
# Filters (pandas)
# ----------------------
def sel_months(df, months=[12,1,2]):
    return df[df['time'].dt.month.isin(months)]

def min_heatwave_days(df, min_days=20):
    # --- FINAL, CORRECTED IMPLEMENTATION ---
    if 'EHF_flag' not in df.columns or df['EHF_flag'].sum() == 0:
        print("WARNING: 'EHF_flag' column not found or contains no heatwave events. Skipping 'min_heatwave_days' filter.")
        return df

    # Create a temporary DataFrame containing only the rows with heatwave events.
    heatwave_df = df[df['EHF_flag'] == 1].copy()
    
    # Add a normalized date column for counting unique days.
    heatwave_df['date'] = pd.to_datetime(heatwave_df['time']).dt.normalize()
    
    # This is a robust way to count unique days per DUID.
    # It groups by DUID, then counts the number of unique 'date' entries in each group.
    heatwave_days = heatwave_df.groupby('DUID')['date'].nunique()

        # --- NEW: Print the calculated heatwave days for each DUID ---
    print("\n--- DEBUG: Calculated Heatwave Days per Generator ---")
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # Ensure all rows are printed
        print(heatwave_days.sort_values(ascending=False))
    print("--- END DEBUG ---\n")
    # -------------------------------------------------------------
    
    
    # Get the DUIDs that meet the threshold.
    valid_duids = heatwave_days[heatwave_days >= min_days].index
    
    if valid_duids.empty:
        print(f"WARNING: No generators met the minimum of {min_days} heatwave days.")
        print("         The 'min_heatwave_days' filter will result in an empty DataFrame.")
        # We return an empty frame here because the user explicitly asked for this filter.
        # If no generators pass, the correct result is an empty DataFrame.
        return df[df['DUID'].isin(valid_duids)].copy()
    
    print(f"INFO: Applying 'min_heatwave_days' filter. Keeping {len(valid_duids)} of {df['DUID'].nunique()} generators.")
    
    # Filter the original DataFrame and return it with its structure unchanged.
    return df[df['DUID'].isin(valid_duids)].copy()

def remove_negatives(df):
    return df[df['TOTALMWh'] >= 0]

def remove_solar_night(df):
    solar_mask = df['fuel_source_primary'] == 'Solar'
    times = df.loc[solar_mask, 'time'].dt.time
    time_filter = ~(
        (times >= datetime.time(21, 0)) |
        (times < datetime.time(5, 0))
    )

    non_solar = df.loc[~solar_mask].copy()
    solar = df.loc[solar_mask].copy()
    solar = solar.loc[time_filter]

    return pd.concat([non_solar, solar], ignore_index=True)

def remove_wind_zeros(df, threshold=40):
    wind_df = df[df['fuel_source_primary'] == 'Wind']
    non_wind_df = df[df['fuel_source_primary'] != 'Wind']

    if wind_df.empty:
        return df

    percent_zeros = (
        wind_df.groupby('DUID')['TOTALMWh']
               .apply(lambda x: (x == 0).sum() / len(x) * 100)
    )
    
    keep_duids = percent_zeros[percent_zeros <= threshold].index
    
    if keep_duids.empty and non_wind_df.empty:
        print(f"WARNING: All wind generators exceeded the zero-MWh threshold of {threshold}%.")
        print("The 'remove_wind_zeros' filter would result in an empty DataFrame. Skipping filter.")
        return df

    wind_filtered = wind_df[wind_df['DUID'].isin(keep_duids)]
    return pd.concat([non_wind_df, wind_filtered], ignore_index=True)

def clear_agc(df):
    df = df[~((df['TOTALMWh'] < 0))].copy()
    mask = (df['fuel_source_primary'].isin([
            'Water', 'Natural Gas Pipeline', 'Black Coal', 'Coal Seam Methane',
            'Brown Coal', 'Diesel', 'Kerosene'
        ]) & (df['TOTALCLEARED'] <= 0))
    df.loc[mask, 'TOTALMWh'] = np.nan
    return df

# ----------------------
# Main entry point
# ----------------------
def load_generation_data(
    sdate,
    edate,
    mode="daily",
    state=None,
    ftype=None,
    apply_sel_months=False,
    months=[12,1,2],
    apply_remove_negatives=False,
    apply_remove_wind_zeros=False,
    wind_zero_threshold=40,
    apply_solar_night=False,
    apply_min_heatwave_days=False,
    min_heatwave_days_threshold=20,
    apply_clear_agc=False
    ):
    
    sdate = pd.to_datetime(sdate, utc=True)
    edate = pd.to_datetime(edate, utc=True)
    t0 = time.time()
    
    gen_details_dtype = {
        'DUID': 'object',
        'max_cap_mw': 'object',
        'reg_cap_mw': 'object',
        'reg_cap_mw_nmap': 'object'
    }
    gen_details_dd = dd.read_csv(GEN_INFO_FP, dtype=gen_details_dtype)
    
    hw_tseries_dd = dd.read_csv(HW_FP, dtype={'DUID': 'string'})
    hw_tseries_dd['time'] = dd.to_datetime(hw_tseries_dd['time'], utc=True)
    hw_tseries_dd['date'] = hw_tseries_dd['time'].dt.normalize()
    
    print(f"Read gen_details & hw_tseries with Dask: {time.time()-t0:.2f} sec")

    t1 = time.time()
    grp_dd = select_group(gen_details_dd, state=state, ftype=ftype)
    print(f"Select group: {time.time()-t1:.2f} sec")

    t2 = time.time()
    grp_pd = grp_dd.compute() 
    df, grp = process_group_dask(grp_pd, grp_dd, GEN_FPATH, hw_tseries_dd, sdate, edate, mode)
    print(f"Process group: {time.time()-t2:.2f} sec")

    t3 = time.time()
    if apply_sel_months:
        df = sel_months(df, months=months)
    if apply_remove_negatives:
        df = remove_negatives(df)
    if apply_remove_wind_zeros:
        df = remove_wind_zeros(df, threshold=wind_zero_threshold)
    if apply_solar_night:
        df = remove_solar_night(df)
    if apply_min_heatwave_days:
        df = min_heatwave_days(df, min_days=min_heatwave_days_threshold)
    if apply_clear_agc:
        df = clear_agc(df)
    print(f"Filters: {time.time()-t3:.2f} sec")

    if not df.empty:
        final_duids = df['DUID'].unique()
        grp = grp[grp['DUID'].isin(final_duids)].copy()
    else:
        # If df is empty, grp should also be empty.
        grp = grp.iloc[0:0].copy()
    # --------------------------------------------------------------------

    return df, grp