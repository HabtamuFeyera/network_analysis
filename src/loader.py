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

    def get_tagged_users(self, df):
        return df['msg_content'].map(lambda x: re.findall(r'@U\w+', x))

    def get_community_participation(self):
        combined = []
        comm_dict = {}
        for json_file in glob.glob(os.path.join(self.path, '*.json')):
            with open(json_file, 'r') as slack_data:
                combined.append(json.load(slack_data))
        for i in combined:
            if not isinstance(i, list):
                print(f"Invalid JSON format in file: {i}")
                continue
            for msg in i:
                if 'replies' in msg.keys():
                    for reply in msg['replies']:
                        comm_dict[reply['user']] = comm_dict.get(reply['user'], 0) + 1
        return comm_dict

    def get_messages_from_channel(self, channel_path):
        channel_json_files = os.listdir(channel_path)
        channel_msgs = [json.load(open(os.path.join(channel_path, f), 'r', encoding='utf-8')) for f in channel_json_files]

        df = pd.concat([pd.DataFrame(self.get_messages_dict(msgs)) for msgs in channel_msgs])
        print(f"Number of messages in channel: {len(df)}")

        return df

    def get_messages_dict(self, msgs):
        # Placeholder: Implement this function based on specific data structure.
        # It's not clear from the provided code how the messages are structured.
        # You might need to adjust this function accordingly.
        # Example: Return a dictionary with keys like 'msg_type', 'msg_content', etc.
        pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')
    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()

    if args.zip:
        slack_loader = SlackDataLoader(args.zip)
        # Call the necessary methods to load and process the Slack data
        user_map = slack_loader.get_user_map()
        tagged_users = slack_loader.get_tagged_users(df)  # Replace df with your DataFrame
        community_participation = slack_loader.get_community_participation()
        # Add more method calls as needed
    else:
        print("Please provide a zip file.")
