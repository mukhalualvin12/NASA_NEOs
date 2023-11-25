import pandas as pd
import requests
from datetime import datetime, timedelta
import seaborn as sns
import matplotlib.pyplot as plt
import warnings
import os


class NearEarthObjects:
    def __init__(self, api_key, start_date=None, finish_date=None):
        self.api_key = api_key
        if start_date is None or finish_date is None:
            warnings.warn("""Start Date or End date has not been supplied. 
            Using today's data as start date and 7 days in the future as end date""")
            start_dt = datetime.today()
            end_dt = start_dt + timedelta(days=7)
            self.start_date = start_dt.strftime('%Y-%m-%d')
            self.finish_date = end_dt.strftime('%Y-%m-%d')
        else:
            self.start_date = start_date
            self.finish_date = finish_date

    def get_neos_by_date_range(self):
        """ Capture the number of Near-Earth Objects over a maximum 7 day date range from NASA

        Returns:
            DataFrame: dataframe of near-earth objects over that date range and associated data from NASA
        Raises:
            ValueError: Raises this if date format is not 'YYYY-MM-DD' or if the date range is not 7 days or less
        Example:
            my_neos = get_neos_by_date_range(start_date='2022-01-01', finish_date='2022-01-07')

        """

        fmt = '%Y-%m-%d'
        # Check date format is correct
        try:
            day_diff = datetime.strptime(self.finish_date, fmt) - datetime.strptime(self.start_date, fmt)
            day_diff_days = day_diff.days
        except ValueError:
            print('Date format does not seem correct. Please make sure it is in the form YYYY-MM-DD')

        # Check objects in a date range
        if day_diff_days > 7:
            alternative_end_date = datetime.strptime(self.start_date, fmt) + timedelta(days=7)
            alternative_end_date_str = alternative_end_date.strftime('%Y-%m-%d')
            raise ValueError(
                f"""Please make sure the date range is at most 7 days. It is currently {day_diff_days} days. 
                Try {alternative_end_date_str} and below"""
            )

        # Read in data
        neo_date_range_url = f'https://api.nasa.gov/neo/rest/v1/feed?start_date={self.start_date}&end_date={self.finish_date}&api_key={self.api_key}'
        neo_data = requests.get(neo_date_range_url)

        if neo_data.status_code != 200:
            raise ValueError(f"Status code: {neo_data.status_code}. Look at JSON: {neo_data}")

        # Handle data and convert to data frame
        neos = neo_data.json()['near_earth_objects']

        clean_df = pd.DataFrame()
        for keys, values in neos.items():
            for list_item in values:
                # Handling unnested keys
                unnested_general_keys = ['links',
                                         'id',
                                         'name',
                                         'nasa_jpl_url',
                                         'absolute_magnitude_h',
                                         'is_potentially_hazardous_asteroid',
                                         'is_sentry_object']
                temp_unnested_dict = {k: list_item[k] for k in unnested_general_keys}
                unnested_general_df = pd.DataFrame(temp_unnested_dict)
                unnested_general_df.reset_index(drop=True, inplace=True)

                # Handling nested size data - dict of dicts
                size_dict = list_item['estimated_diameter']
                size_df = pd.DataFrame()
                for k_size, v_size in size_dict.items():
                    mini_df = pd.DataFrame(v_size, index=range(0, 1))
                    cols_fix = ['size_' + k_size + '_' + x for x in list(mini_df.columns)]
                    mini_df.columns = cols_fix
                    size_df = pd.concat([size_df, mini_df], axis=1)

                # Handling unnested relative positioning data
                approach_data_dict = list_item['close_approach_data'][0]
                unnested_approach_keys = ['close_approach_date_full',
                                          'orbiting_body']
                unnested_approach_dict = {k: approach_data_dict[k] for k in unnested_approach_keys}
                approach_df = pd.DataFrame(unnested_approach_dict, index=range(0, 1))

                # Handling nested relative positioning data
                nested_approach_keys = ['relative_velocity',
                                        'miss_distance']
                nested_approach_df = pd.DataFrame()
                for ap in nested_approach_keys:
                    nested_approach_temp_df = pd.DataFrame(approach_data_dict[ap], index=range(0, 1))
                    nested_cols_fixed = [ap + '_' + x for x in nested_approach_temp_df.columns]
                    nested_approach_temp_df.columns = nested_cols_fixed
                    nested_approach_df = pd.concat([nested_approach_temp_df, nested_approach_df], axis=1)

                # Merge all columns together
                list_of_dfs = [unnested_general_df, size_df, approach_df, nested_approach_df]
                combined_df = pd.concat(list_of_dfs, axis=1)
                clean_df = pd.concat([clean_df, combined_df], axis=0)

        return clean_df


# Static variables
api = os.environ.get("NASA_API") # API provided when running using run configuration instead of console run
today_date = datetime.today()
later_date = today_date + timedelta(days=7)

today = today_date.strftime("%Y-%m-%d")
later = later_date.strftime("%Y-%m-%d")

future_start_date = '2024-01-01'
future_end_date = '2024-01-07'

cols_transform = ['miss_distance_astronomical',
                  'miss_distance_lunar',
                  'miss_distance_kilometers',
                  'miss_distance_miles',
                  'relative_velocity_kilometers_per_second',
                  'relative_velocity_kilometers_per_hour',
                  'relative_velocity_miles_per_hour']

# Asteroids and NEO by date
neos_now_obj = NearEarthObjects(start_date=today, finish_date=later, api_key=api)
neos_now = neos_now_obj.get_neos_by_date_range()
neos_now['close_approach_date_full'] = pd.to_datetime(neos_now['close_approach_date_full'])
neos_now['tag'] = 'This Week'
neos_now[cols_transform] = neos_now[cols_transform].astype(float)

neos_later_obj = NearEarthObjects(start_date=future_start_date, finish_date=future_end_date, api_key=api)
neos_later = neos_later_obj.get_neos_by_date_range()
neos_later['close_approach_date_full'] = pd.to_datetime(neos_later['close_approach_date_full'])
neos_later['tag'] = 'Future Week'
neos_later[cols_transform] = neos_later[cols_transform].astype(float)

total_table = pd.concat([neos_now, neos_later])

# Plotting proximity, size and danger over the next 7 days
sns.scatterplot(
    data=neos_now,
    x='close_approach_date_full',
    y='miss_distance_kilometers',
    size='size_kilometers_estimated_diameter_max',
    style='is_potentially_hazardous_asteroid',
    hue='relative_velocity_kilometers_per_second',
    palette='viridis'
)

# Boxplot to check velocities
sns.boxplot(
    data=total_table,
    x='relative_velocity_kilometers_per_second'
)

# Pair Grid to investigate all parameters about asteroids
var_list = ['size_kilometers_estimated_diameter_max',
            'miss_distance_kilometers',
            'relative_velocity_kilometers_per_second',
            'absolute_magnitude_h']

sns.pairplot(
    data=total_table,
    vars=var_list,
    kind='scatter',
    diag_kind='kde',
    hue='is_potentially_hazardous_asteroid',
    palette='husl',
    plot_kws={
        'alpha': 0.75
    }
)

plt.show()