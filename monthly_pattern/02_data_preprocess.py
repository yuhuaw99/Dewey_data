import pandas as pd
import os
import ast
from joblib import Parallel, delayed, parallel_backend
import concurrent.futures


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

# read file and extract requried field
def read_file(file_path):
    df = pd.read_csv(file_path, compression='gzip')
    df.columns = df.columns.str.lower()
    df = df[['naics_code', 'poi_cbg', 'visitor_home_cbgs']]
    df = df.dropna()
    return df

# replace lambda funtion with named function
def get_naics_short(x):
    return str(x)[:3]

def join_strings(x):
    return ','.join(x)

def get_dict_values(d):
    return list(d.values())

def get_dict_keys(d):
    return list(d.keys())


# replace category and filter out needed ones
def gen_category(df, poi_category=poi_category):
    df.loc[:, 'naics_short'] = df['naics_code'].apply(get_naics_short)
    df.loc[:, 'category'] = df['naics_short'].replace(poi_category)
    df = df[df['category'].isin(poi_category_list)]
    df = df[['category','poi_cbg', 'visitor_home_cbgs']]
    return df


# aggregate poi level data to bg level 
def aggregate(df):
    df = df.groupby(['poi_cbg', 'category']).agg(join_strings).reset_index()
    df.loc[:, 'visitor_home_cbgs'] = df['visitor_home_cbgs'].str.replace(r'(?<=.)[{}](?=.)', '', regex=True) # Use regex to remove any { or } that is not at the start or end
    return df


# transform original dataframe to od dataframe
def transform_od(df):
    df.loc[:, 'visitor_home_cbgs'] = df['visitor_home_cbgs'].apply(ast.literal_eval)
    df['visitor_count'] = df['visitor_home_cbgs'].apply(get_dict_values)
    df['visitor_home_cbgs'] = df['visitor_home_cbgs'].apply(get_dict_keys)
    df = df.explode(['visitor_home_cbgs', 'visitor_count'])
    return df


# pre aggregate data to reduce file size
def pre_aggregate(df):
    df = df.groupby(['visitor_home_cbgs','category', 'poi_cbg']).agg({'visitor_count':'sum'})
    return df

# wrap function for preprocess
def preprocess(df, poi_category=poi_category):
    df = gen_category(df, poi_category)
    df = aggregate(df)
    df = transform_od(df)
    df = pre_aggregate(df)
    return df 


# write csv to file
def write_file(df, file_path):
    df.to_csv(file_path)


# wrap funtion for all
def excute(file_path_list):
    df = read_file(file_path_list[0])
    df = preprocess(df)
    write_file(df, file_path_list[1])
    print(file_path_list[0]+'is done!')
    print('#'*20)


def main():
    year = 2019
    os.chdir(f'{year}_files')
    in_file_list = [file for file in os.listdir('raw') if 'Foot' in file]
    out_file_list = [file[:-3] for file in in_file_list]
    file_list = list(zip(in_file_list, out_file_list))
    os.makedirs(f'{year}_preprocessed',exist_ok=True)
    file_list = [[f'raw/{x}', f'{year}_preprocessed/{y}'] for x,y in file_list]

    # with parallel_backend('loky', n_jobs=-1):
    #     Parallel()(delayed(excute(x)) for x in file_list)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        executor.map(excute, file_list)

if __name__ == '__main__':
    main()