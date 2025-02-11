import polars as pl
from datetime import datetime, timedelta

df = pl.read_csv('../rawdata/Amphibians_observations.csv')

today = datetime.today().strftime('%Y-%m-%d')
time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

# for col_name, dtype in df.schema.items():
#     print(f"Column: {col_name}, Type: {dtype}")


columns_to_delete = ['observed_on_string', 'time_zone', 'quality_grade', 'url', 'positional_accuracy', 'tag_list',
                     'private_place_guess', 'private_latitude', 'private_longitude', 'captive_cultivated',
                     'public_positional_accuracy', 'geoprivacy', 'taxon_geoprivacy', 'coordinates_obscured',
                     'positioning_method', 'positioning_device', 'place_town_name', 'taxon_id']


df = df.drop([col for col in columns_to_delete if col in df.columns])


###
df = df.with_columns([
    pl.col('observed_on').fill_null(today),
    pl.col('description').fill_null('Not Provided'),
    pl.col('num_identification_agreements').fill_null(0),
    pl.col('num_identification_disagreements').fill_null(0),
    pl.col('latitude').fill_null(float('nan')),
    pl.col('longitude').fill_null(float('nan')),
    pl.col('place_guess').fill_null('Not Provided'),
    pl.col('place_county_name').fill_null('Not provided'),
    pl.col('place_state_name').fill_null('Not provided'),
    pl.col('place_country_name').fill_null('India'),
    pl.col('species_guess').fill_null('Not provided'),
    pl.col('common_name').fill_null('Not provided'),
    pl.col('scientific_name').fill_null('Not provided'),
    pl.col('iconic_taxon_name').fill_null('Amphibia'),
    pl.col('time_observed_at').fill_null(f"{time} UTC"),
])




### Observation Time ###
time_of_observation = df["time_observed_at"].to_list()

extracted_time = []
for t in time_of_observation:
    if t:
        extracted_time.append(t[-12:-4])



df = df.with_columns(
    pl.Series(extracted_time).alias('time_observed_at')
)


df = df.with_columns([
    pl.col('time_observed_at').str.strptime(pl.Datetime, format="%H:%M:%S")
])


df = df.with_columns([
    (pl.col('time_observed_at') + timedelta(hours=5, minutes=30)).alias('time_observed_at'),
])

df = df.with_columns([
    pl.col('time_observed_at').dt.strftime('%H:%M:%S').alias('time_observed_at')
])

df = df.with_columns([
    pl.col('time_observed_at').str.strptime(pl.Time, format="%H:%M:%S")
])


### Created and Updated date  ####
df = df.with_columns([
    pl.col('created_at').str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S UTC"),
    pl.col('updated_at').str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S UTC"),
])

df = df.with_columns([
    (pl.col('created_at') + timedelta(hours=5, minutes=30)).alias('created_at'),
    (pl.col('updated_at') + timedelta(hours=5, minutes=30)).alias('updated_at'),
])


 #### Writing data to Csv #####
df.write_csv(file='../filtered_data/filtered_amphibians_observations.csv', include_header=True)


print(df.select('observed_on', 'created_at', 'updated_at', 'time_observed_at'))















