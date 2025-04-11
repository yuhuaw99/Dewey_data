import pandas as pd
import concurrent.futures
import os

def read_csv_file(file_path):
    # assign category as category column to save memory use
    dtype_dict = {
        'category':'category',
        'poi_cbg':'int64',
        'visitor_home_cbgs':'str',
        'visitor_count':'int32'
    }
    df = pd.read_csv(file_path, dtype=dtype_dict)
    df = df[~df['visitor_home_cbgs'].str.contains('CA')]
    df.loc[:, 'visitor_home_cbgs'] = df['visitor_home_cbgs'].astype('int64')
    df.loc[:, 'id'] = df['visitor_home_cbgs'].astype('str') + df['category'].astype('str') + df['poi_cbg'].astype('str')

    return df

def parallel_read_and_concat(file_paths):
    print('Start reading files')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        dfs = list(executor.map(read_csv_file, file_paths))
    print('Files have been read, start concating......')
    print()
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

def main():
    year = 2019
    os.chdir(f'{year}_files')
    files = os.listdir(f'{year}_preprocessed')
    months = [str(x).zfill(2) for x in range(1,6)]
    os.makedirs(f'{year}_monthly', exist_ok=True)
    for month in months:
        file_paths = [f'{year}_preprocessed/{file}' for file in files if f'{month}-01' in file]
        # print(file_paths[0:10])
        # print('------------------')
        # print(file_paths)
        combined_df = parallel_read_and_concat(file_paths)
        combined_df = combined_df[['id', 'poi_cbg', 'category', 'visitor_home_cbgs', 'visitor_count']]
        print('')
        combined_df = combined_df.groupby('id').agg({'visitor_count':'sum',
                                                    'poi_cbg':'first',
                                                    'visitor_home_cbgs':'first',
                                                    'category':'first'}).reset_index()
        print('Aggregating done, start writing file!')
        combined_df.to_csv(f'{year}_monthly/'+'monthly_pattern_'+month+'.csv')
        print('File has been saved!')
        del combined_df
        print('#'*20)
as

if __name__ == '__main__':
    main()