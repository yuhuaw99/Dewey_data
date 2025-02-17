import pandas as pd
import os
from census import Census
import concurrent.futures
import geopandas as gpd



def filter_file(file):
    print('Read crosswalk data')
    # read msa2county cross walk 
    msa_county_cw = pd.read_excel('cbsa2fipsxw.xlsx', dtype={'cbsacode':'str'})
    # msa_county_cw = pd.read_excel('cbsa2fipsxw.xlsx')

    # create county column
    msa_county_cw.loc[:, 'countyfips'] = msa_county_cw['fipsstatecode'].apply(lambda x: str(x).zfill(2)) + msa_county_cw['fipscountycode'].apply(lambda x: str(x).zfill(3))

    # filter out only msa
    msa_county_cw = msa_county_cw[msa_county_cw['metropolitanmicropolitanstatis'] == 'Metropolitan Statistical Area']

    # filter out Hawaii and Alaska and Puerto Rico
    msa_county_cw = msa_county_cw[~msa_county_cw['fipsstatecode'].isin([2, 15, 72])]

    # select needed column
    msa_county_cw = msa_county_cw[['cbsacode', 'countyfips']]


    ###########################################################
    print('Read msa_pop file.....')
    # read msa_pop file
    msa_pop = pd.read_csv('msa_pop_20.csv', dtype={'msa':'str'})

    # groupby msa code to get overall pop
    msa_pop = msa_pop.groupby('msa').agg({'pop':'sum'}).reset_index()

    # filter out only msa 
    msa_pop = msa_pop[msa_pop['msa'].isin(msa_county_cw['cbsacode'].unique())]

    # get top 100 msa
    msa_pop = msa_pop.sort_values('pop', ascending=False).reset_index(drop=True)


    #############################################################
    print('read msa file')
    msa = gpd.read_file('../../General_data/msa_20.gpkg')
    # filter msa 
    msa_pop = msa_pop[msa_pop['msa'].isin(msa['GEOID'])].head(100)
    msa_county_cw = pd.merge(msa_county_cw, msa_pop, left_on='cbsacode', right_on='msa', how='inner')

    # transform to dict
    msa_county_cw = dict(zip(msa_county_cw['countyfips'], msa_county_cw['cbsacode']))


    #############################################################
    # read month mobility pattern
    print('Start reading file '+file)
    df_mp = pd.read_csv('2024_monthly/'+file, dtype={'category':'category'}) # save memory use by pre-assign column type

    # get county fipscode for both poi and bg
    df_mp.loc[:, 'poi_county'] = df_mp['poi_cbg'].apply(lambda x: str(x).zfill(12)[0:5])
    df_mp.loc[:, 'bg_county'] = df_mp['visitor_home_cbgs'].apply(lambda x: str(x).zfill(12)[0:5])

    # get msa for both poi and bg
    df_mp.loc[:, 'poi_msa'] = df_mp['poi_county'].map(msa_county_cw)
    df_mp.loc[:, 'bg_msa'] = df_mp['bg_county'].map(msa_county_cw)

    # filter data within each msa
    df_mp = df_mp[df_mp['poi_msa'] == df_mp['bg_msa']]
    df_mp = df_mp[df_mp['poi_msa'].isin(msa['GEOID'])]
    print(f'The dataframe contains {len(df_mp['poi_msa'].unique())} MSAs')

    # save file to disk
    df_mp.rename(columns={'bg_msa':'msa'}, inplace=True)
    df_mp = df_mp[['poi_cbg', 'category', 'visitor_home_cbgs', 'visitor_count','msa', 'id']]
    print('Filtering done, start writing '+file)
    print(f'{file} contains total of {len(df_mp['msa'].unique())} MSAs')
    df_mp.to_csv('2024_monthly_msa/'+file)
    print(f'Writing {file} done with {len(df_mp)} rows')


def main():
    # list all files need to be filtered
    files = os.listdir('2024_monthly')
    with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(filter_file, files)

if __name__ == '__main__':
     main()