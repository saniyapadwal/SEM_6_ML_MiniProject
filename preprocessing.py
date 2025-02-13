import polars as pl

df = pl.read_csv(f"CompleteDataset/wild_life_all_species.csv")

print(df.shape)

df = df.drop(["description", "no_identification_agreement", "no_identification_disagreement", "country", "user_id", "updated_date"])

print(df.shape)



df1 = pl.read_csv("CompleteDataset/updated_wild_life_all_species.csv")
print(df1.shape)

columns_to_check = ["latitude", "longitude", "image", "location", "city", "species_name_guess", "scientific_name", "common_name", "city", "state", "location"]

for columns in columns_to_check:
    dataType = df1.schema[columns]
    if dataType == "Float64":
        df1 = df1.filter(
            ~pl.col(columns).is_nan() & ~pl.col(columns).is_null() 
        )
    elif dataType == "String":
        df1 = df1.filter(
            ~pl.col(columns).is_null() 
        )

for columns in columns_to_check:
    if df1.schema[columns] == pl.Utf8:
        df1 = df1.filter(pl.col(columns) != "Not provided")
        
    
    
print(df1.shape)


df1.write_csv(f"CompleteDataset/updated_wild_life_all_species.csv", include_header=True)

