import pandas as pd
import datetime
import re
import constants
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

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

def plot_with_not(df, notification_df):

    fig = go.Figure()
    for col in df.columns: 
        fig.add_trace(go.Scatter(x=df.index, y=df[col], mode='lines', name=col))

    for start_time in notification_df['startTime']:
        fig.add_vrect(x0=start_time -  pd.Timedelta(days=3), x1=start_time + pd.Timedelta(days=3), line_width=0, fillcolor="red", opacity=0.2)
    fig.show()

def plot_sampling_rate(data, sample_rate, value_counts):
    # two series

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=data.index, y=data, name=data.name),
        secondary_y=False,
    )

    fig.add_trace(
        go.Scatter(x=sample_rate.index, y=sample_rate, name="sampling rate"),
        secondary_y=True,
    )

    # Set x-axis title
    fig.update_xaxes(title_text="Timestamp")

    # Set y-axes titles
    fig.update_yaxes(title_text="rpm", secondary_y=False)
    fig.update_yaxes(title_text="count", secondary_y=True)

    fig.show()

    fig = px.histogram(value_counts, x = value_counts['index'], y=value_counts['rpm'],
                    marginal="box", # or violin, rug
                    )
    fig.show()