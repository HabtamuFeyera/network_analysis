import json
import argparse
import os
import pandas as pd
import glob


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

    def get_channel_messages(self, channel_name):
        messages = []
        channel_id = None 

        for channel in self.channels:
            if channel['name'] == channel_name:
                channel_id = channel['id']
                break
        if channel_id is None:
            print(f"channel '{channel_name}' not found")
            return messages 

        channel_folder_path = os.path.join(self.path, channel_name)
        for filename in os.listdir(channel_folder_path):
            if filename.endswith('.json'):
                file_path = os.path.join(channel_folder_path, filename)
                with open(file_path, 'r') as f:
                    channel_messages = json.load(f)
                    messages.extend(channel_messages)
        return messages 

    def get_user_map(self):
        user_names_by_id = {}
        user_ids_by_name = {}

        for user in self.users:
            user_names_by_id[user['id']] = user['name']
            user_ids_by_name[user['name']] = user['id']

        return user_names_by_id, user_ids_by_name        

    
    # ... (the rest of the class remains the same)

    def slack_parser(self, path_channel):
        combined = []
        for json_file in glob.glob(f"{path_channel}/*.json"):
            with open(json_file, 'r', encoding="utf8") as slack_data:
                slack_data = json.load(slack_data)
                combined.append(slack_data)

        msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
        reply_count, reply_users_count, tm_thread_end = [], [], [], [], [], [], [], [], [], []

        for slack_data in combined:
            for row in slack_data:
                if 'type' in row and 'bot_id' not in row.keys():
                    msg_type.append(row['type'])
                    msg_content.append(row['text'])
                    sender_id.append(row.get('user_profile', {}).get('real_name', 'Not provided'))
                    time_msg.append(row['ts'])
                    msg_dist.append(row['blocks'][0]['elements'][0]['elements'][0]['type']
                                    if ('blocks' in row.keys() and
                                        len(row['blocks'][0]['elements'][0]['elements']) != 0) else 'reshared')
                    time_thread_st.append(row['thread_ts'] if 'thread_ts' in row.keys() else 0)
                    reply_users.append(",".join(row['reply_users']) if 'reply_users' in row.keys() else 0)
                    reply_count.append(row['reply_count'] if 'reply_count' in row.keys() else 0)
                    reply_users_count.append(row['reply_users_count'] if 'reply_users_count' in row.keys() else 0)
                    tm_thread_end.append(row['latest_reply'] if 'latest_reply' in row.keys() else 0)

        data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st,
                   reply_count, reply_users_count, reply_users, tm_thread_end)
        columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type',
                   'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']

        df = pd.DataFrame(data=data, columns=columns)
        df = df[df['sender_name'] != 'Not provided']
        df['channel'] = path_channel.split('/')[-1].split('.')[0]
        df = df.reset_index(drop=True)

        return df

    # ... (the rest of the class remains the same)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')
    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()
