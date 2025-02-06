import pandas as pd
import os
from census import Census
import concurrent.futures



def filter_file(file):
    print('Download data from Census....')
    # set up for census
    API_KEY = "c45c87b10be4ee416ac0e7acde8def5fb840398c"
    c = Census(API_KEY)

    employment_var = 'B01003_001E'

    # get census data
    msa_employment = c.acs5.get(
        ('NAME', employment_var),
        {'for': 'metropolitan statistical area/micropolitan statistical area:*'}
    )

    # select top 100
    msa_100 = pd.DataFrame(msa_employment).sort_values(by=employment_var, ascending=False).head(100)

    # rename column
    msa_100.columns = ['msa', 'pop', 'cbsacode']

    # change column type
    msa_100.loc[:, 'cbsacode'] = msa_100['cbsacode'].astype('int64')

    print('Read crosswalk data.....')
    # read msa2county cross walk 
    msa_county_cw = pd.read_excel('cbsa2fipsxw.xlsx')

    # select only msa 
    msa_county_cw = msa_county_cw[msa_county_cw['metropolitanmicropolitanstatis']=='Metropolitan Statistical Area']

    # create county column
    msa_county_cw.loc[:, 'countyfips'] = msa_county_cw['fipsstatecode'].apply(lambda x: str(x).zfill(2)) + msa_county_cw['fipscountycode'].apply(lambda x: str(x).zfill(3))

    # select needed column
    msa_county_cw = msa_county_cw[['cbsacode', 'countyfips']]


    # filter cw
    msa_county_cw = msa_county_cw[msa_county_cw['cbsacode'].isin(msa_100['cbsacode'])]

    # transform to dict
    msa_county_cw = dict(zip(msa_county_cw['countyfips'], msa_county_cw['cbsacode']))

    # read month mobility pattern
    print('Start reading file'+file)
    df_mp = pd.read_csv('2024_monthly/'+file, dtype={'category':'category'}) # save memory use by pre-assign column type

    # get county fipscode for both poi and bg
    df_mp.loc[:, 'poi_county'] = df_mp['poi_cbg'].apply(lambda x: str(x).zfill(12)[0:5])
    df_mp.loc[:, 'bg_county'] = df_mp['visitor_home_cbgs'].apply(lambda x: str(x).zfill(12)[0:5])

    # get msa for both poi and bg
    df_mp.loc[:, 'poi_msa'] = df_mp['poi_county'].map(msa_county_cw)
    df_mp.loc[:, 'bg_msa'] = df_mp['bg_county'].map(msa_county_cw)

    # filter data within each msa
    df_mp = df_mp[df_mp['poi_msa'] == df_mp['bg_msa']]

    # save file to disk
    df_mp.rename(columns={'bg_msa':'msa'}, inplace=True)
    df_mp = df_mp[['poi_cbg', 'category', 'visitor_home_cbgs', 'visitor_count','msa', 'id']]
    print('Filtering done, start writing'+file)
    df_mp.to_csv('2024_monthly_msa/'+file)
    print(f'Writing {file} done with {len(df_mp)} rows')


def main():
    # list all files need to be filtered
    files = os.listdir('2024_monthly')
    with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(filter_file, files)

if __name__ == '__main__':
     main()