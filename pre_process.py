
import pandas as pd
import plotly.express as px
import os
import constants
import utils

def create_subsets(df, subset_intervals, folder_name):
    print('creating subsets...')
    labels = df.label.unique()
    subsets = []
    for interval in subset_intervals: subsets.append(df[df['label'].isin(labels[interval[0]:interval[1]])])
    
    for subset in subsets: print(subset.label.unique())
    if input('ok? ') == 'y':
        print(subsets[0])

        if input('ok? ') == 'y':
            filenames = []
            for subset in subsets: filenames.append(utils.create_file_name(subset.iloc[0].label))
            print(filenames)

            if input('ok? ') == 'y':
                folder_path = f'data/{folder_name}/'
                #Assuming the folder does not exists, and therefore no files with the same names
                if not os.path.exists(folder_path):
                    os.mkdir(folder_path)
                    print(f'saving subsets in folder {folder_path}')
                    for filename in filenames: subset.to_parquet(f'{folder_path}/{filename}.parquet')
                else:
                    print('cannot save files since they already exists')


def create_units_and_sampling_df():
    sensor_list = pd.read_csv('data/sensor_list_v2.txt', sep=':', header=None, names=['label', 'sampling time'])
    sensor_units = pd.read_csv('data/sensor_units.txt', sep='delimiter', header=None, names=['unit'])
    sensor_list['unit'] = sensor_units['unit'].str.split().str[-1]

    sensor_list['label'] = sensor_list['label'].str[:-22] # remove the " average sampling time"
    sensor_list['sampling time'] = sensor_list['sampling time'].str[:-8].astype('float64')

    sensor_list['category'] = sensor_list.apply(lambda row : utils.category(row), axis=1)
    sensor_list['label'] = sensor_list.apply(lambda row : utils.re_label(row), axis=1)
    sensor_list.loc[sensor_list['label'] == "RPM", "label"] = 'Motor RPM'

    return sensor_list



def create_notifications_df(filename = "IAA_29PA0002_and_children_notifications_m2.xlsx"):
    df = pd.read_excel("data/"+filename)
    
    new_columns = df.columns[0].split(',')[1:]# first will be index
    new_columns.remove('metadata')
    notifications_df = pd.DataFrame(columns=new_columns)

    # iterate through 
    for i in range(len(df)):
        string = df.iloc[i][0]
        row = utils.split_string(string)
        notifications_df.loc[len(notifications_df)] = row

    
    # translating timestamps to datetime
    time_columns = [column for column in notifications_df.columns if 'Time' in column ]
    for column in time_columns:
        notifications_df[column] = notifications_df[column].apply(lambda timestamp: utils.translate_timestamp_to_datetime(timestamp))
    return notifications_df


def run(mode = constants.MOTOR):
    if mode == constants.MOTOR:
        filename = "Motor.parquet"
        subset_intervals = [[0,1], [1,3], [3,5], [5,8], [8,10]]
    elif mode == constants.PUMP_PROCESS:
        filename = "Pump Process.parquet"
        subset_intervals = [[0,2], [2,3], [3,4], [4,6], [6,7]]
    elif mode == constants.PUMP_MONITORING:
        filename = "Pump Monitoring (BN).parquet"
        subset_intervals = [[0,2], [2,4], [4,6], [6,8], [8,12], [12,14]]

    print('fetching data...')
    df = pd.read_parquet("data/"+filename)
    if mode == constants.MOTOR:
        df[df.label == 'NDE Vibration X plane ']
        df[df.label == 'NDE Vibration X plane']
    create_subsets(df, subset_intervals, mode)

#run(MOTOR)
#run(constants.PUMP_PROCESS)
