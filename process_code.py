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
HW_FP = "/scratch/ng72/ms5578/time_series/gen_hw_status.csv"
GEN_FPATH = "/scratch/ng72/ms5578/time_series/nem_generation"

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

import time
import os
import pandas as pd
import numpy as np
import dask.dataframe as dd

def process_group_dask(grp_pd, grp_dd, gen_fpath, hw_tseries_dd, start_date, end_date, mode="daily"):
    timings = {}
    print("--- Starting Dask-Native Process ---")
    
    # 1. Get safe DUIDs from the computed pandas DataFrame
    t0 = time.time()
    safe_duids = set(duid.replace("/", "_").replace("\\", "_") for duid in grp_pd['DUID'])
    timings['duid_setup'] = time.time() - t0

    # 2. Bulk Dask read_csv
    t0 = time.time()
    dfs = dd.read_csv(
        f"{gen_fpath}/*.csv",
        dtype={'DUID': 'string'},
        assume_missing=True,
        blocksize="256MB"
    )
    timings['csv_bulk_read'] = time.time() - t0

    # 3. Filter DUIDs
    t0 = time.time()
    dfs = dfs[dfs['DUID'].isin(safe_duids)]
    dfs = dfs[dfs['DUID'] != 'DUID']
    timings['filter_and_clean'] = time.time() - t0

    # 4. Type conversions and dropping nulls
    t0 = time.time()
    dfs['time'] = dd.to_datetime(dfs['time'], errors='coerce')
    dfs['TOTALCLEARED'] = dd.to_numeric(dfs['TOTALCLEARED'], errors='coerce')
    dfs['TOTALMWh'] = dd.to_numeric(dfs['TOTALMWh'], errors='coerce')
    dfs['AGCSTATUS'] = dd.to_numeric(dfs['AGCSTATUS'], errors='coerce').fillna(0).astype(int)
    dfs = dfs.dropna(subset=['time', 'DUID'])
    timings['type_conversion_and_dropna'] = time.time() - t0
    
    # 5. Filter by date, then set and sort the index
    t0 = time.time()
    dfs = dfs[(dfs['time'] >= start_date) & (dfs['time'] <= end_date)]
    print("Setting index and sorting data by time... (This may take a while)")
    dfs = dfs.set_index('time')
    timings['date_filter_and_sort'] = time.time() - t0

    # 6. Prepare heatwave time series
    t0 = time.time()
    hw_tseries_dd['time'] = dd.to_datetime(hw_tseries_dd['time']).dt.normalize()
    hw_tseries_dd = hw_tseries_dd.set_index('time').loc[start_date:end_date]
    timings['hw_tseries_filter'] = time.time() - t0

    # 7. Aggregation/groupby
    t0 = time.time()
    if mode == 'hourly':
        dfs_hourly = dfs.reset_index()
        hw_daily_dd = hw_tseries_dd.reset_index()
        hw_daily_dd['date'] = hw_daily_dd['time'].dt.normalize()
        dfs_hourly['date'] = dfs_hourly['time'].dt.normalize()
        merged = dd.merge(
            dfs_hourly, hw_daily_dd.drop(columns='time'), on=['DUID', 'date'], how='left'
        ).drop(columns='date')
        timings['aggregate_hourly'] = time.time() - t0
    else: # daily mode
        agg_func = {'TOTALMWh': 'sum', 'TOTALCLEARED': 'sum', 'AGCSTATUS': 'max'}
        dfs_daily = dfs.groupby(['DUID', dd.Grouper(freq='1D')]).agg(agg_func).reset_index()
        hw_daily_dd = hw_tseries_dd.reset_index()
        dfs_daily['time'] = dd.to_datetime(dfs_daily['time']).dt.normalize()
        hw_daily_dd['time'] = dd.to_datetime(hw_daily_dd['time']).dt.normalize()
        merged = dd.merge(dfs_daily, hw_daily_dd, on=['DUID', 'time'], how='left')
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
    
    # --- FIX: Restore the return statement ---
    return df_computed.reset_index(drop=True), grp_pd_jittered.reset_index(drop=True)

# ----------------------
# Filters (pandas)
# ----------------------
def sel_months(df, months=[12,1,2]):
    return df[df['time'].dt.month.isin(months)]

def min_heatwave_days(df, min_days=20):
    df = df.copy()
    df['date'] = pd.to_datetime(df['time']).dt.normalize()
    heatwave_days = (
        df[df['EHF_flag'] == 1]
        .groupby('DUID')['date']
        .nunique()
    )
    valid_duids = heatwave_days[heatwave_days >= min_days].index
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
    percent_zeros = (
        wind_df.groupby('DUID')['TOTALMWh']
               .apply(lambda x: (x == 0).sum() / len(x) * 100)
    )
    keep_duids = percent_zeros[percent_zeros <= threshold].index
    wind_filtered = wind_df[wind_df['DUID'].isin(keep_duids)]
    non_wind_df = df[df['fuel_source_primary'] != 'Wind']
    return pd.concat([non_wind_df, wind_filtered], ignore_index=True)

def clear_agc(df):
    # Removes days where AGCSTATUS is 0 and the plant is non-operational.
    # Generally ill-advised
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
    t0 = time.time()
    
    gen_details_dtype = {
        'DUID': 'string',
        'max_cap_mw': 'object',
        'reg_cap_mw': 'object',
        'reg_cap_mw_nmap': 'object'
    }
    gen_details_dd = dd.read_csv(GEN_INFO_FP, dtype=gen_details_dtype)
    
    # --- FIX: Specify dtype directly in read_csv for hw_tseries ---
    # This prevents the subtle type mismatch warning by ensuring the
    # dtype is consistent from the moment the file is read.
    hw_tseries_dd = dd.read_csv(HW_FP, dtype={'DUID': 'string'})
    
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
    return df, grp