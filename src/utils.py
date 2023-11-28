import json
import glob
import pandas as pd
import datetime
import re

def combine_json_files(file_paths):
    """Combine data from multiple JSON files."""
    combined_data = []
    for file_path in file_paths:
        with open(file_path, 'r', encoding="utf8") as slack_data:
            combined_data.extend(json.load(slack_data))
    return combined_data


def convert_2_timestamp(column, data):
    """Convert from unix time to readable timestamp."""
    if column in data.columns.values:
        timestamp_ = []
        for time_unix in data[column]:
            if time_unix == 0:
                timestamp_.append(0)
            else:
                a = datetime.datetime.fromtimestamp(float(time_unix))
                timestamp_.append(a.strftime('%Y-%m-%d %H:%M:%S'))
        return timestamp_
    else:
        print(f"{column} not in data")


def get_tagged_users(df):
    """Get all @ in the messages."""
    return df['msg_content'].map(lambda x: re.findall(r'@U\w+', x))


def get_community_participation(path):
    """Get community participation."""
    combined = []
    comm_dict = {}
    for json_file in glob.glob(f"{path}*.json"):
        with open(json_file, 'r') as slack_data:
            combined.append(slack_data)

    for i in combined:
        a = json.load(open(i.name, 'r', encoding='utf-8'))
        for msg in a:
            if 'replies' in msg.keys():
                for i in msg['replies']:
                    comm_dict[i['user']] = comm_dict.get(i['user'], 0) + 1
    return comm_dict

