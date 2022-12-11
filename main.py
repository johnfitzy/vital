from io import BytesIO
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

import pandas as pd

file_url = 'https://nztaopendata.blob.core.windows.net/motorvehicleregister/Fleet-data-all-vehicle-years.zip'
input_file_csv = "Fleet-30Nov2022.csv"
input_file_parquet = "Fleet-30Nov2022.parquet"


def download_file():
    if not Path(input_file_csv).is_file():
        print(f'Downloading file: {input_file_csv}')
        with urlopen(file_url) as zipresp:
            with ZipFile(BytesIO(zipresp.read())) as zfile:
                zfile.extractall('/usr/src/app/')
    else:
        print(f'File: {input_file_csv} already exists')


def convert_to_parquet():
    if not Path(input_file_parquet).is_file():
        print(f'Cleaning and converting file to: {input_file_parquet}....')
        pd.read_csv(input_file_csv, dtype={"TRANSMISSION_TYPE": str, "SYNTHETIC_GREENHOUSE_GAS": str}) \
            .to_parquet(input_file_parquet, compression=None)


def filter_data_from_1950(input_dataframe):
    return input_dataframe[input_dataframe['FIRST_NZ_REGISTRATION_YEAR'] >= 1950]


def filter_data_unwanted_makes(input_dataframe):
    return input_dataframe[~input_dataframe['MAKE'].isin(['TRAILER', 'CARAVAN', 'FACTORY BUILT', 'HOMEBUILT', 'BRIFORD'])]


def filter_data_cars_only(input_dataframe):
    return input_dataframe[~input_dataframe['VEHICLE_TYPE'].isin(['PASSENGER CAR/VAN|'])]


def do_calc(input_dataframe, start, end):
    year_dfs = []
    for year in range(start, end):
        grouped_by_make_df = input_dataframe[input_dataframe['FIRST_NZ_REGISTRATION_YEAR'] == year].groupby('MAKE').size() \
            .reset_index(name='NUMBER_OF_REGISTRATIONS') \
            .sort_values(by=['NUMBER_OF_REGISTRATIONS'], ascending=False)

        grouped_by_make_df.insert(0, 'FIRST_NZ_REGISTRATION_YEAR', year)
        year_dfs.append(grouped_by_make_df.head(20))

    return pd.concat(year_dfs)


if __name__ == '__main__':
    # set up
    download_file()
    # more efficient to convert to parquet
    convert_to_parquet()

    # data processing
    df = pd.read_parquet(input_file_parquet)
    df = filter_data_from_1950(df)
    df = filter_data_unwanted_makes(df)
    df = filter_data_cars_only(df)

    result = do_calc(df, 1950, 2023)

    result.to_csv("result.csv.gz", index=False, compression='gzip')
