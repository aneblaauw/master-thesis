
import pandas as pd
import plotly.express as px
import os
import constants
import utils

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit

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

def create_subsets_pyspark(df, subset_intervals, folder_name, save = False):
    print('creating subsets...')
    labels = [row['label'] for row in  sorted(df.select('label').distinct().collect())]
    subsets = {}
    filenames = []
    for interval in subset_intervals: 
        key = utils.create_file_name(labels[interval[0]])
        subsets[key] = df.filter(df.label.isin(labels[interval[0]:interval[1]]))
        filenames.append(key)
    '''
    if save:
        folder_path = f'data/{folder_name}/'
        #Assuming the folder does not exists, and therefore no files with the same names
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
            print(f'saving subsets in folder {folder_path}')
            for i in range(len(subsets)):
                subsets[i].write.parquet(f'{folder_path}/{filenames[i]}.parquet')
        else:
            print('cannot save files since they already exists')'''
    return subsets


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



def create_notifications_df(filename = "IAA_29PA0002_and_children_notifications_m2.xlsx", local_path = ''):
    df = pd.read_excel(local_path+"data/"+filename)
    
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

def filter_and_resample(df, start_date = '2017-03-01', end_date = '2017-09-01'): 
    # todo: find all unique labels in df and split up based on these
    filtered_dates = df.filter((df["timestamp"] >= lit(start_date)) & (df["timestamp"] <= lit(end_date)))
    labels = [row['label'] for row in  sorted(df.select('label').distinct().collect())]
  
    for idx, label in enumerate(labels):
        tmp = filtered_dates.filter(filtered_dates['label'] == label).toPandas().set_index('timestamp').resample('60min').mean().interpolate()
        if idx == 0:
            new_df = pd.DataFrame(index=tmp.index)
        new_df[label] = tmp['data']
        

    #a = filtered_dates.filter(filtered_dates['label'] == 'DE Bearing Temp A').toPandas().set_index('timestamp').resample('60min').mean().interpolate()
    #b = filtered_dates.filter(filtered_dates['label'] == 'DE Bearing Temp B').toPandas().set_index('timestamp').resample('60min').mean().interpolate()

    #new_df = pd.DataFrame(index=a.index)
    #new_df['DE Bearing Temp A'] = a['data']
    #new_df['DE Bearing Temp B'] = b['data']
    return new_df

def run(mode = constants.MOTOR, engine = 'SPARK', save = False , local_path = ''):
    if engine == 'PANDAS':
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
    if engine == 'SPARK':
        spark = SparkSession.builder \
                            .config("spark.executor.memory", "70g") \
                            .config("spark.driver.memory", "50g") \
                            .config("spark.memory.offHeap.enabled",True) \
                            .config("spark.memory.offHeap.size","16g") \
                            .appName('Master-thesis') \
                            .getOrCreate()

        if mode == constants.MOTOR:
            filename = "Aize-student-project-Motor.parquet"
            subset_intervals = [[0,2], [2,4], [4,7], [7,10], [10,12]]
        
        if mode == constants.PUMP_PROCESS:
            filename = "Aize-student-project-Pump Process.parquet"
            subset_intervals = [[0,1], [1,2], [2,3], [3,4], [4,6], [6,9], [9,11], [11,12], [12,13]]
        
        if mode == constants.PUMP_MONITORING:
            filename = "Aize-student-project-Pump Monitoring (BN).parquet"
            subset_intervals = [[0,2], [2,4], [4,6], [6,8], [8,12], [12,14]]
            
            
        print('fetching data...')
        df = spark.read.parquet(local_path+"data/"+filename)

        if mode == constants.MOTOR:
            df = df.withColumn("label", when(df.label == "Motor RPM","rpm").otherwise(df.label))
        
        if mode == constants.PUMP_MONITORING:
            df = df.withColumn("label", when(df.label == "Outet shaft vibration Y plane","Outlet shaft vibration Y plane").otherwise(df.label))
        
        return create_subsets_pyspark(df, subset_intervals, mode, save)
            

#run(constants.MOTOR)
#df = run(constants.PUMP_MONITORING)

'''for key,value in df.items():
    print(filter_and_resample(value))'''

