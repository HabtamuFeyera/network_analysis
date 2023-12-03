import json
import argparse
import os
import glob
import pandas as pd
import re

class SlackDataLoader:
    def __init__(self, path):
        self.path = path
        self.channels = self.get_channels()
        self.users = self.get_users()

    def get_users(self):
        with open(os.path.join(self.path, 'users.json'), 'r') as f:
            users = json.load(f)
        return users

    def get_channels(self):
        with open(os.path.join(self.path, 'channels.json'), 'r') as f:
            channels = json.load(f)
        return channels

    def get_user_map(self):
        user_names_by_id = {}
        user_ids_by_name = {}
        for user in self.users:
            user_names_by_id[user['id']] = user['name']
            user_ids_by_name[user['name']] = user['id']
        return user_names_by_id, user_ids_by_name

    def get_tagged_users(df):
        return df['msg_content'].map(lambda x: re.findall(r'@U\w+', x))

    def get_community_participation(path):
        combined = []
        comm_dict = {}
        for json_file in glob.glob(f"{path}*.json"):
            with open(json_file, 'r') as slack_data:
                combined.append(slack_data)
        for i in combined:
            a = json.load(i)
            if not isinstance(a, list):
                print(f"Invalid JSON format in file: {i.name}")
                continue
            for msg in a:
                if 'replies' in msg.keys():
                    for i in msg['replies']:
                        comm_dict[i['user']] = comm_dict.get(i['user'], 0) + 1
        return comm_dict

    def get_messages_from_channel(channel_path):
        channel_json_files = os.listdir(channel_path)
        channel_msgs = [json.load(open(os.path.join(channel_path, f), 'r', encoding='utf-8')) for f in channel_json_files]

        df = pd.concat([pd.DataFrame(get_messages_dict(msgs)) for msgs in channel_msgs])
        print(f"Number of messages in channel: {len(df)}")

        return df

    def get_messages_dict(msgs):
        # You should implement this function based on your specific data structure.
        # It's not clear from the provided code how the messages are structured.
        # You might need to adjust this function accordingly.

        # Example: Return a dictionary with keys like 'msg_type', 'msg_content', etc.
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')
    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()
