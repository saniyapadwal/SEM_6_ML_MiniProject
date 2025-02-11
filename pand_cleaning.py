import os
import pandas as pd

df = pd.read_csv('filtered_amphibians_observations.csv')

df = df.drop(columns=['id'])


agreements = 'user_name'


              #### Deleting  username == none ####
df = df[~(df[agreements].isna() | (df[agreements] == ""))]


empty_description_row1 = df[df[('%s' % agreements)].isna()]
empty_description_row2 = df[df[agreements] == ""]
print(empty_description_row1[agreements], empty_description_row2[agreements])


columns = ['observed_on', 'time_observed_at', 'latitude', 'longitude', 'image_url', 'created_at', 'updated_at',
           'description', 'num_identification_agreements', 'num_identification_disagreements', 'place_guess',
           'place_county_name', 'place_state_name', 'place_country_name' 'species_guess', 'scientific_name',
           'common_name', 'iconic_taxon_name']


for c in df.columns:
    if c == 'place_country_name':
        # Replace empty strings with pd.NA, then fill with "India" for this specific column
        df[c] = df[c].replace("", pd.NA).fillna("India")

    # Replace empty strings with pd.NA and then fill with "Not Provided" for all columns
    df[c] = df[c].replace("", pd.NA).fillna("Not Provided")

    # Apply .str.strip() only if the column's dtype is 'object' (string)
    if df[c].dtype == 'object':
        df[c] = df[c].str.strip()


# df.insert(15, 'place_country_name', 'India')

# df = df.iloc[:, [2, 3, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 28, 29, 30, 31, 32, 33, 34]]
df = df.to_csv('../filtered_data/filtered_amphibians_observations.csv', index=False)
























# empty_description_rows = df[df['species_guess'] == "Not Provided"]
# print(empty_description_rows['species_guess'])


# def download_image(image_url, filename):
#     response = requests.get(image_url)
#     if response.status_code == 200:
#         with open(f"images/{filename}", 'wb') as file:
#             file.write(response.content)
#         print(f"Image successfully downloaded as {filename}")
#     else:
#         print(f"Failed to download image from {image_url}")
#
#
# num_rows_to_process = 10
#
# # for index in range(min(num_rows_to_process, len(df))):
# #     rows = df.iloc[index]  # Access the row using ilo
# #     image_url = rows['image_url']
# #     filename = f"{index}_{rows['common_name']}.jpg"
# #     # print(filename)
# #     download_image(image_url=image_url, filename=filename)
#
# # filepath = os.listdir('images')
# #
# # for f in filepath:
# #     full_filepath = os.path.join('images', f)
# #
# #     # with open(full_filepath, 'rb') as file:
# #     #     content = file.read()
# #
# #     # Now iterate through the DataFrame to match the common name with the file name
# #     for index in range(min(num_rows_to_process, len(df))):
# #         row = df.iloc[index]
# #         common_name = f"{index}_{row['common_name']}.jpg"  # Common name to match with filename
# #
# #         # Check if the file matches the common name
# #         if f == common_name:
# #             print(f"Match found for {f}, updating image_url")
# #             df.at[index, 'image_url'] = f  # Update the DataFrame with the file name
#
# df = df['image_url'][0]
# print(df)