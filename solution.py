from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from io import BytesIO
from pandas.tseries.offsets import MonthEnd

azure_connection_string = r"https://raisademo2.blob.core.windows.net/raisa-task-2022?sp=rle&st=2022-12-19T12:20:30Z&se=2023-01-30T20:20:30Z&spr=https&sv=2021-06-08&sr=c&sig=KsKkOKOXJXdAaDnwuw3RefIXuDuY5kITmc%2F7NXt7rpI%3D"
container_name = 'raisa-task-2022'


def get_blobs_client_with_prefix(
        source_container_connection_string,
        source_container_name,
        prefix
):
    container_client = ContainerClient.from_container_url(
        source_container_connection_string)
    blobs_list = container_client.list_blobs(
        name_starts_with=prefix,
    )

    blob_client_list = [container_client.get_blob_client(
        blob['name']) for blob in blobs_list]
    return blob_client_list


def read_parquet_from_blobs_client_list(
    blob_client_list
):
    df = pd.DataFrame()
    for blob in blob_client_list:
        stream_downloader = blob.download_blob()
        stream = BytesIO()
        stream_downloader.readinto(stream)
        processed_df = pd.read_parquet(stream, engine='pyarrow')
        df = pd.concat(
            [df, processed_df]
        )
    return df


def main():
    blobs_list = get_blobs_client_with_prefix(
    azure_connection_string, container_name, 'well_monthly_production/part')

    df = read_parquet_from_blobs_client_list(blobs_list)
    df.reset_index(drop=True, inplace=True)


    # insert missing production dates rows
    new_df = df.iloc[0:1].copy()
    for i, row in df[1:].iterrows(): 
        expected_date = new_df['production_date'][len(new_df)-1] + MonthEnd()
        last_well_id = new_df['well_id'][len(new_df)-1]
        current_date = df['production_date'][i] 
        current_well_id = df['well_id'][i] 
        if last_well_id == current_well_id:
            if expected_date != current_date :
                while expected_date != current_date:
                    new_df.loc[len(new_df)]= {'well_id': current_well_id , 'production_date': expected_date,
                            'oil_production':0}
                    expected_date += MonthEnd()
                    
        new_df.loc[len(new_df)] = df.iloc[i]

    # Flagging down months by adding 'down_flag' column
    new_df['down_flag']=[True if new_df['oil_production'][0] <=25 else False ] + [True if (new_df['oil_production'][i] <= 25 or\
                                (new_df['oil_production'][i]-new_df['oil_production'][i-1]) <= -30\
                                ) else False for i in range(1,len(new_df['oil_production']))]

    # selectin unique well ids in array to calculate te downtime for each well separately
    well_ids = new_df.well_id.unique()
    

    # creating the solution dataframe
    solution_df = pd.DataFrame(columns=[
    'well_id',
                'downtime_start_date',
                'downtime_end_date',
                'downtime_duration'
    ])
    # will fetch each well data and process it
    for id in well_ids:
        group = new_df[new_df['well_id'] == id].reset_index()
        # will select the down days only to check if the down days are consecutive or not (duration > 1)
        group = group[group['down_flag']==True]
        group.reset_index(inplace=True)

        current_start = group['production_date'][0]
        current_duration = 1
        expected_next_date= current_start + MonthEnd()

        for i in range(1,len(group)):
            if group['production_date'][i] == expected_next_date:
                expected_next_date += MonthEnd()
                current_duration += 1
            else:            
                dt = {
                    'well_id': group['well_id'][i],
                    'downtime_start_date': current_start.replace(day=1),
                    'downtime_end_date' : (expected_next_date - MonthEnd() ).replace(day=1),
                    'downtime_duration':current_duration
                }
                if current_duration > 1:
                    solution_df.loc[len(solution_df)-1] = dt
                    
                current_start = group['production_date'][i]
                expected_next_date =group['production_date'][i] + MonthEnd()
                current_duration = 1

    solution_df.to_csv('solution.csv')

if __name__=="__main__":
    main()