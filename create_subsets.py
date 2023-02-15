
import pandas as pd
import plotly.express as px
import os


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
            for subset in subsets: filenames.append(create_file_name(subset.iloc[0].label))
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





# utils
def create_file_name(name):
    word_list = ['A', 'X', 'plane', '"U"', '"V"', '"W"', "Motor", '(PST)']
    char_list = ['?']
    res = '_'.join([idx for idx in name.split() if idx not in word_list]).lower()
    for char in char_list:
        res = res.replace(char, '')
    
    return res




MOTOR = 'motor'
PUMP_PROCESS = 'pump_process'
PUMP_MONITORING = 'pump_monitoring'

def run(mode = MOTOR):
    if mode == MOTOR:
        filename = "Motor.parquet"
        subset_intervals = [[0,1], [1,3], [3,5], [5,8], [8,10]]
    elif mode == PUMP_PROCESS:
        filename = "Pump Process.parquet"
        subset_intervals = [[0,2], [2,3], [3,4], [4,6], [6,7]]
    elif mode == PUMP_MONITORING:
        filename = "Pump Monitoring (BN).parquet"
        subset_intervals = [[0,2], [2,4], [4,6], [6,8], [8,12], [12,14]]

    print('fetching data...')
    df = pd.read_parquet("data/"+filename)
    if mode == MOTOR:
        df[df.label == 'NDE Vibration X plane ']
        df[df.label == 'NDE Vibration X plane']
    create_subsets(df, subset_intervals, mode)

#run(MOTOR)
run(PUMP_PROCESS)