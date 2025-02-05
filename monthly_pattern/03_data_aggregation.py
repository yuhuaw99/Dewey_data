import pandas as pd
import concurrent.futures
import os

def read_csv_file(file_path):
    # assign category as category column to save memory use
    dtype_dict = {
        'category':'category'
    }
    return pd.read_csv(file_path, dtype=dtype_dict)

def parallel_read_and_concat(file_paths):
    with concurrent.futures.ProcessPoolExecutor() as executor:
        dfs = list(executor.map(read_csv_file, file_paths))

    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df


if __name__ == '__main__':
    files = os.listdir('2024_01_05_preprocessed')
    months = [str(x).zfill(2) for x in range(1,6)]
    for month in months:
        file_paths = [f'2024_01_05_preprocessed/{file}' for file in files if f'{month}-01' in file]
        # print(file_paths[0:10])
        # print('------------------')
        # print(file_paths)
        combined_df = parallel_read_and_concat(file_paths)
        combined_df = combined_df[['poi_cbg', 'category', 'visitor_home_cbgs', 'visitor_count']]
        combined_df.to_csv('2024_01_05_monthly/'+'monthly_pattern_'+month+'.csv')
        del combined_df