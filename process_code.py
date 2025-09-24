import os
import datetime
import pandas as pd
import numpy as np

# ----------------------
# Constants for filepaths
# ----------------------
GEN_INFO_FP = "/g/data/ng72/ms5578/ID_HW_BARRA/data/preprocess/gen_info.csv"
HW_FP = "/scratch/ng72/ms5578/time_series/gen_hw_status.csv"
GEN_FPATH = "/scratch/ng72/ms5578/time_series/nem_generation"

# ----------------------
# Selection helpers
# ----------------------
def select_group(gen_details, state=None, ftype=None):
    if state is not None and ftype is not None:
        if isinstance(ftype, list):
            grp = gen_details[(gen_details['region'] == state) & (gen_details['fuel_source_primary'].isin(ftype))]
        else:
            groups = gen_details.groupby(['region', 'fuel_source_primary'])
            grp = groups.get_group((state, ftype))
    elif state is not None:
        groups = gen_details.groupby('region')
        grp = groups.get_group(state)
    elif ftype is not None:
        if isinstance(ftype, list):
            grp = gen_details[gen_details['fuel_source_primary'].isin(ftype)]
        else:
            groups = gen_details.groupby('fuel_source_primary')
            grp = groups.get_group(ftype)
    else:
        grp = gen_details
    return grp

# ----------------------
# Processing
# ----------------------
def process_group(grp, gen_fpath, hw_tseries, start_date, end_date, mode="daily"):
    safe_duids = [duid.replace("/", "_").replace("\\", "_") for duid in grp['DUID']]
    gen_locs = [f"{gen_fpath}/{duid}.csv" for duid in safe_duids]
    dfs = [pd.read_csv(fp, dtype='object') for fp in gen_locs if os.path.exists(fp)]
    if not dfs:
        raise ValueError("No generator CSV files found.")

    dfs = pd.concat(dfs, ignore_index=True)
    header_row = dfs.columns.tolist()
    dfs = dfs[~dfs.apply(lambda row: list(row) == header_row, axis=1)]

    dfs['time'] = pd.to_datetime(dfs['time'])
    dfs['TOTALCLEARED'] = dfs['TOTALCLEARED'].astype(float)
    dfs['TOTALMWh'] = dfs['TOTALMWh'].astype(float)
    dfs['AGCSTATUS'] = pd.to_numeric(dfs['AGCSTATUS'], errors='coerce').fillna(0).astype(int)

    dfs = dfs.set_index('time').sort_index()
    dfs = dfs.loc[start_date:end_date]

    hw_tseries['time'] = pd.to_datetime(hw_tseries['time']).dt.normalize()
    hw_tseries = hw_tseries.set_index('time').sort_index()
    hw_tseries = hw_tseries.loc[start_date:end_date]

    if mode == 'daily':
        agg_func = {'TOTALMWh': 'sum', 'TOTALCLEARED': 'sum', 'AGCSTATUS': 'max'}
        dfs_daily = dfs.groupby(['DUID', pd.Grouper(freq='1D')]).agg(agg_func).reset_index()
        hw_tseries_daily = hw_tseries.reset_index()
        dfs_daily['time'] = pd.to_datetime(dfs_daily['time']).dt.normalize()
        hw_tseries_daily['time'] = pd.to_datetime(hw_tseries_daily['time']).dt.normalize()
        merged = pd.merge(dfs_daily, hw_tseries_daily, on=['DUID', 'time'], how='left')
    elif mode == 'hourly':
        dfs_hourly = dfs.reset_index()
        duids = dfs_hourly['DUID'].unique()
        hourly_range = pd.date_range(start=start_date, end=end_date, freq='1h')
        full_index = pd.MultiIndex.from_product([duids, hourly_range], names=['DUID', 'time'])
        hw_hourly = pd.DataFrame(index=full_index).reset_index()
        hw_hourly['date'] = hw_hourly['time'].dt.normalize()
        hw_tseries_daily = hw_tseries.reset_index()
        hw_tseries_daily['date'] = pd.to_datetime(hw_tseries_daily['time']).dt.normalize()
        hw_tseries_broadcast = pd.merge(
            hw_hourly,
            hw_tseries_daily.drop(columns='time'),
            on=['DUID', 'date'],
            how='left'
        ).drop(columns='date')
        merged = pd.merge(dfs_hourly, hw_tseries_broadcast, on=['DUID', 'time'], how='left')
    else:
        raise ValueError("mode must be 'daily' or 'hourly'")

    merged = merged.dropna(how='all')

    def jitter(group):
        group = group.copy()
        duplicates = group.groupby(['lat', 'lon']).cumcount()
        np.random.seed(42)
        jitter_strength = 0.05
        group['lat_jittered'] = group['lat'] + np.random.uniform(-jitter_strength, jitter_strength, size=len(group)) * duplicates
        group['lon_jittered'] = group['lon'] + np.random.uniform(-jitter_strength, jitter_strength, size=len(group)) * duplicates
        return group

    grp = jitter(grp)

    df = merged.merge(
        grp[['DUID', 'fuel_source_primary', 'region', 'lat_jittered', 'lon_jittered']],
        on='DUID',
        how='left'
    )

    return df.reset_index(drop=True), grp.reset_index(drop=True)

# ----------------------
# Filters (optional)
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
    apply_clear_agc=False   # <- renamed for consistency
):
    gen_details = pd.read_csv(GEN_INFO_FP)
    hw_tseries = pd.read_csv(HW_FP)
    
    grp = select_group(gen_details, state=state, ftype=ftype)
    df, grp = process_group(grp, GEN_FPATH, hw_tseries, sdate, edate, mode)

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

    return df, grp

