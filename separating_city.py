import polars as pl

df = pl.read_csv("./wild_life_all_species.csv")

# Drop rows where 'city' is null
df = df.drop_nulls(subset=['city'])

# Get unique city names and ensure they are strings
cities = df['city'].drop_nulls().unique().cast(pl.Utf8)


for city in cities:
    city_df = df.filter(df['city'] == city)
    city_df.write_csv(f"cities/{city}.csv")