import pandas as pd
import os 

file_list = os.listdir('2024_preprocessed')

df = [pd.read_csv(f'2024_preprocessed/{file}', index_col=0, dtype={'GEOID':'str'}) for file in file_list]

df = pd.concat(df)

df = df.groupby('GEOID').sum()

df.to_csv('poi.csv')