import json
import argparse
import os
import io
import shutil
import copy
from datetime import datetime
from pick import pick
from time import sleep

class SlackDataLoader:
    def __init__(self, data_path):
        # Your implementation for data loading goes here
        pass

    def load_data(self):
        # Your implementation to load and return data goes here
        pass


def combine_json_files(file_paths):
    """Combine data from multiple JSON files."""
    combined_data = []
    for file_path in file_paths:
        with open(file_path, 'r', encoding="utf8") as slack_data:
            combined_data.extend(json.load(slack_data))
    return combined_data


def slack_parser(path_channels):
    """ parse slack data to extract useful information from the json file
        step of execution
        1. Import the required modules
        2. read all json file from the provided path
        3. combine all json files in the provided path
        4. extract all required information from the slack data
        5. convert to dataframe and merge all
        6. reset the index and return dataframe
    """
    dflist = []

    for path_channel in path_channels:
        combined_data = combine_json_files(glob.glob(f"{path_channel}*.json"))

        msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
            reply_count, reply_users_count, tm_thread_end = [], [], [], [], [], [], [], [], [], []

        for row in combined_data:
            if 'bot_id' in row.keys():
                continue
            else:
                msg_type.append(row['type'])
                msg_content.append(row['text'])
                if 'user_profile' in row.keys():
                    sender_id.append(row['user_profile']['real_name'])
                else:
                    sender_id.append('Not provided')
                time_msg.append(row['ts'])
                if 'blocks' in row.keys() and len(row['blocks'][0]['elements'][0]['elements']) != 0:
                    msg_dist.append(row['blocks'][0]['elements'][0]['elements'][0]['type'])
                else:
                    msg_dist.append('reshared')
                if 'thread_ts' in row.keys():
                    time_thread_st.append(row['thread_ts'])
                else:
                    time_thread_st.append(0)
                if 'reply_users' in row.keys():
                    reply_users.append(",".join(row['reply_users']))
                else:
                    reply_users.append(0)
                if 'reply_count' in row.keys():
                    reply_count.append(row['reply_count'])
                    reply_users_count.append(row['reply_users_count'])
                    tm_thread_end.append(row['latest_reply'])
                else:
                    reply_count.append(0)
                    reply_users_count.append(0)
                    tm_thread_end.append(0)

        data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st,
                   reply_count, reply_users_count, reply_users, tm_thread_end)
        columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type',
                   'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']

        df = pd.DataFrame(data=data, columns=columns)
        df = df[df['sender_name'] != 'Not provided']
        dflist.append(df)

    dfall = pd.concat(dflist, ignore_index=True)
    dfall['channel'] = path_channels[0].split('/')[-1].split('.')[0]
    dfall = dfall.reset_index(drop=True)

    return dfall
