import datetime
import re
import constants

def translate_timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(int(timestamp) / 1e3)
def create_file_name(name):
    res = '_'.join([idx for idx in name.split() if idx not in constants.WORD_LIST]).lower()
    for char in constants.CHAR_LIST:
        res = res.replace(char, '')
    
    return res

def category(row):
    for cat in constants.CATEGORIES: 
        if cat in row['label']:
            return cat

def re_label(row):
    for category in constants.CATEGORIES:
        if category in row['label']:
            return row['label'].replace(category+' ','')

def split_string(string):
    split_string = re.split('[{}]', string)
    # metadata = split_string[1] # waiting with metadata
    res = (split_string[0][:-2] + ',' + split_string[2][2:]).split(',')[1:]
    if len(res) == 13:
        res[6:8] = [' '.join(res[6:8])]
    return res
