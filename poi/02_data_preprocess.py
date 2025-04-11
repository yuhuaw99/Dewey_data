import pandas as pd
import os
import ast
import geopandas as gpd
from joblib import Parallel, delayed, parallel_backend
import concurrent.futures

os.environ['GDAL_DATA'] = r'C:\Users\yuhua\miniforge3\Library\share\gdal'
# poi_category and its naics code
poi_category = {
    '445': 'Maintenance', # Supermarket, food retailer, liquor
    '449': 'Matainance', # Furniture and appliance
    '455': 'Maintenance', # Department store, warehouse clubs
    '456': 'Health', # Health and personal care store
    '458': 'Maintenance', # Clothing, shoe, and jewelry
    '459': 'Maintenance', # Sporting goods, hobby, musical instrument, etc.
    '611': 'Education', # Schools, colleges, etc.
    '621': 'Health', # Ambulatory health care
    '622': 'Health', # Hospitals
    '623': 'Health', # Nursing 
    '624': 'Health', # Social assistance
    '711': 'Recreation', # Performing arts
    '712': 'Recreation', # Museum
    '713': 'Recreation', # Amusement
    '722': 'Dining', # Food services
    '811': 'Maintenance', # Repair and maintenance
    '812': 'Maintenance', # Personal care, laundry, etc
    '813': 'Maintenance' # 'Religious'
}

poi_category_list = ['Maintenance', 'Health', 'Education', 'Recreation', 'Dining']

bg = gpd.read_file('../../General_data/bg_10.gpkg')
print('read bg done')


def read_file(file_path):
    df = pd.read_csv(file_path, dtype={'NAICS_CODE':'str'})
    df.columns = df.columns.str.lower()
    df = df[['naics_code', 'longitude', 'latitude']]
    return df


def get_naics_short(x):
    return str(x)[:3]


def gen_category(df, poi_category=poi_category):
    df.loc[:, 'naics_short'] = df['naics_code'].apply(get_naics_short)
    df.loc[:, 'category'] = df['naics_short'].replace(poi_category)
    df = df[df['category'].isin(poi_category_list)]
    df = df[['category','latitude', 'longitude']]
    return df

def aggregate(df, bg=bg):
    df = gpd.GeoDataFrame(
        df, 
        geometry=gpd.points_from_xy(df['longitude'], df['latitude']),
        crs="EPSG:4269"  # CRS equivalent to EPSG 4269 (NAD83)
        )
    intersect = gpd.overlay(df, bg, how='intersection')
    df = intersect.groupby(['GEOID', 'category']).agg(count=('geometry','count')).reset_index()
    df = df.pivot(index='GEOID', columns='category', values='count').reset_index()
    df = df.fillna(0)
    return df

def preprocess(df):
    df = gen_category(df)
    df = aggregate(df)
    return df

def write_file(df, file_path):
    df.to_csv(file_path)


def excute(file_path_list):
    df = read_file(file_path_list[0])
    print(file_path_list[0]+' is loaded')
    df = preprocess(df)
    print(file_path_list[0]+' is preprocessed')
    write_file(df, file_path_list[1])
    print(file_path_list[0]+' is done!')
    print('#'*20)


def main():
    in_file_list = os.listdir('2024')
    out_file_list = [file[:-3] for file in in_file_list]
    file_list = list(zip(in_file_list, out_file_list))
    file_list = [[f'2024/{x}', f'2024_preprocessed/{y}'] for x,y in file_list]
    print(f'file_list is read, for {len(file_list)}')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(excute, file_list)

if __name__ =='__main__':
    main()