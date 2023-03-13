
import pandas as pd
import plotly.express as px
import os
import constants
import utils
from functools import reduce
import numpy as np


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

def filter_and_resample(df, start_date = constants.TIMESPAN[0], end_date = constants.TIMESPAN[1]): 
    # todo: find all unique labels in df and split up based on these
    filtered_dates = df.filter((df["timestamp"] >= lit(start_date)) & (df["timestamp"] <= lit(end_date)))
    labels = [row['label'] for row in  sorted(df.select('label').distinct().collect())]
  
    for idx, label in enumerate(labels):
        tmp = filtered_dates.filter(filtered_dates['label'] == label).toPandas().set_index('timestamp')
        tmp['sample_rate'] = 1
        tmp = tmp.resample('60min').agg({'data':"mean", 'sample_rate':"sum"})
        print(f'Number of NaN values for {label}:{tmp["data"].isna().sum()}')
        tmp = tmp.interpolate()
        if idx == 0:
            sample_rate = pd.DataFrame(index=tmp.index)
            new_df = pd.DataFrame(index=tmp.index)
        new_df[label] = tmp['data']
        sample_rate[label] = tmp['sample_rate']
        
    return new_df, sample_rate

def clean_motor_off(df_sensor_list):
    '''_summary_

    _extended_summary_

    Args:
        df_sensor_list (_type_): _description_

    Returns:
        _type_: _description_
    '''
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['timestamp'], how='outer'), df_sensor_list)

    for col in df_merged.columns:
        df_merged[col] = df_merged.apply(lambda row: np.NaN if row['rpm'] == 0 else row[col], axis=1).interpolate() # method='polynomial', order=2
        
    return df_merged

def clean_data(fasit, tbc, threshold = 5):
    '''
    Cleans tbc series based on when datapoints in fasit gets lower than a certain threshold
    Args:
        fasit (pd.Series): Series that defines if the datapoints should be cleaned
        tbc (pd.Series): The series to be cleaned
        threshold (int): Threshold

        returns cleaned series
    '''
    return fasit.to_frame().join(tbc).apply(lambda row: np.NaN if row.iloc[0] <= threshold else row.iloc[1] , axis = 1).interpolate()

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
            subset_intervals = [[0,2], [2,4], [4,6], [6,9], [9,11]]
        
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
            df = df.withColumn("label", when(df.label == "NDE Vibration X plane ","NDE Vibration X plane").otherwise(df.label))
        
        if mode == constants.PUMP_MONITORING:
            df = df.withColumn("label", when(df.label == "Outet shaft vibration Y plane","Outlet shaft vibration Y plane").otherwise(df.label))
        
        return create_subsets_pyspark(df, subset_intervals, mode, save)
            

'''motor_subsets = run(constants.MOTOR)
rpm, _ = filter_and_resample(motor_subsets['rpm'], start_date='2018-01-01', end_date='2018-03-01')
rpm_series = rpm.iloc[:,0]
nde_bearing_temp, _ = filter_and_resample(motor_subsets['nde_bearing_temp'],start_date='2018-01-01', end_date='2018-03-01')
nde_bearing_temp_a_series = nde_bearing_temp.iloc[:,0]'''

'''nde_bearing_temp_a_series = pd.read_csv('data/test_nde_bearing_temp_A', index_col='timestamp').iloc[:,0]
rpm_series= pd.read_csv('data/test_rpm', index_col='timestamp')['rpm']
print(rpm_series)
print(nde_bearing_temp_a_series)
clean_data(rpm_series, nde_bearing_temp_a_series)'''
#df = run(constants.PUMP_MONITORING)

'''for key,value in df.items():
    print(filter_and_resample(value))'''

