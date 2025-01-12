from joblib import Parallel, delayed
import pandas as pd
from datetime import datetime

def process_filtered_data(dataframe):
    valid_rows = Parallel(n_jobs=-1)(delayed(filter_invalid_city)(record) for _, record in dataframe.iterrows())
    filtered_df = dataframe[valid_rows]

    seasonal_stats = filtered_df.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()
    merged_df = filtered_df.merge(seasonal_stats, on=['city', 'season'])

    merged_df['outlier_flag'] = (merged_df['temperature'] > (merged_df['mean'] + 2 * merged_df['std'])) | \
                                (merged_df['temperature'] < (merged_df['mean'] - 2 * merged_df['std']))

    return merged_df

def filter_invalid_city(record):
    return not (
        record['city'] in ['Singapore', 'Sydney', 'Rio de Janeiro'] and
        pd.to_datetime(record['timestamp']) <= pd.to_datetime('2010-01-30')
    )

def identify_season(month):
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    elif month in [9, 10, 11]:
        return "autumn"

def calculate_seasonal_stats(dataframe, city):
    current_month = datetime.now().month
    active_season = identify_season(current_month)
    grouped_stats = dataframe.groupby(['city', 'season'])['temperature'].agg(['mean', 'std']).reset_index()

    mean_temp = grouped_stats[(grouped_stats['city'] == city) &
                              (grouped_stats['season'] == active_season)]['mean']
    std_temp = grouped_stats[(grouped_stats['city'] == city) &
                             (grouped_stats['season'] == active_season)]['std']

    return mean_temp.values[0], std_temp.values[0]

def calculate_monthly_stats(dataframe, city):
    current_month = datetime.now().month
    dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
    dataframe['month'] = dataframe['timestamp'].dt.month

    monthly_stats = dataframe.groupby(['city', 'month'])['temperature'].agg(['mean', 'std']).reset_index()

    monthly_mean = monthly_stats[(monthly_stats['city'] == city) &
                                 (monthly_stats['month'] == current_month)]['mean']
    monthly_std = monthly_stats[(monthly_stats['city'] == city) &
                                (monthly_stats['month'] == current_month)]['std']

    return monthly_mean.values[0], monthly_std.values[0]

def is_temperature_normal(mean, std_dev, current_temp):
    return abs(current_temp - mean) <= 2 * std_dev

def flag_outliers(temperature, upper_limit, lower_limit):
    return (temperature > upper_limit) | (temperature < lower_limit)