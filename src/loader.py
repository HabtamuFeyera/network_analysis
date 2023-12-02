import json
import argparse
import os
import glob
import pandas as pd

class SlackDataLoader:
    '''
    Slack exported data IO class.

    When you open slack exported ZIP file, each channel or direct message 
    will have its own folder. Each folder will contain messages from the 
    conversation, organised by date in separate JSON files.

    You'll see reference files for different kinds of conversations: 
    users.json files for all types of users that exist in the slack workspace
    channels.json files for public channels.

    For security reason, we have anonymized names - the names you will see are generated using faker library.
    '''

    def __init__(self, path):
        '''
        path: path to the slack exported data folder
        '''
        self.path = path
        self.channels = self.get_channels()
        self.users = self.get_users()

    def get_users(self):
        '''
        Get all the users from the json file.
        '''
        try:
            with open(os.path.join(self.path, 'users.json'), 'r') as f:
                users = json.load(f)
            return users
        except FileNotFoundError:
            print("Error: users.json file not found.")
            return []

    def get_channels(self):
        '''
        Get all the channels from the json file.
        '''
        try:
            with open(os.path.join(self.path, 'channels.json'), 'r') as f:
                channels = json.load(f)
            return channels
        except FileNotFoundError:
            print("Error: channels.json file not found.")
            return []

    def get_user_map(self):
        '''
        Get a map between user id and user name.
        '''
        user_names_by_id = {}
        user_ids_by_name = {}

        for user in self.users:
            user_names_by_id[user['id']] = user['name']
            user_ids_by_name[user['name']] = user['id']

        return user_names_by_id, user_ids_by_name

    def slack_parser(self, path_channel):
        '''
        Parse slack data to extract useful information from the json file.
        '''
        combined = []

        for json_file in glob.glob(f"{path_channel}/*.json"):
            with open(json_file, 'r', encoding="utf8") as slack_data:
                try:
                    slack_data = json.load(slack_data)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in file: {json_file}")
                    continue
                
                if not isinstance(slack_data, list):
                    print(f"Invalid JSON format in file: {json_file}")
                    continue
                
                combined.append(slack_data)

        dflist = []

        for slack_data in combined:
            msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st, reply_users, \
            reply_count, reply_users_count, tm_thread_end = [], [], [], [], [], [], [], [], [], []

            for row in slack_data:
                if not isinstance(row, dict):
                    print(f"Invalid data format in row: {row}")
                    continue

                if 'bot_id' not in row.keys():
                    msg_type.append(row.get(['type']))
                    msg_content.append(row.get(['text']))
                    sender_id.append(row.get('user_profile', {}).get('real_name', 'Not provided'))
                    time_msg.append(row.get(['ts']))
                    msg_dist.append(row['blocks'][0]['elements'][0]['elements'][0]['type']
                                     if 'blocks' in row and row['blocks'] and len(row['blocks'][0]['elements'][0]['elements']) != 0
                                     else 'reshared')
                    time_thread_st.append(row.get('thread_ts', 0))
                    reply_users.append(",".join(row.get('reply_users', [])))
                    reply_count.append(row.get('reply_count', 0))
                    reply_users_count.append(row.get('reply_users_count', 0))
                    tm_thread_end.append(row.get('latest_reply', 0))

            data = zip(msg_type, msg_content, sender_id, time_msg, msg_dist, time_thread_st,
                       reply_count, reply_users_count, reply_users, tm_thread_end)
            columns = ['msg_type', 'msg_content', 'sender_name', 'msg_sent_time', 'msg_dist_type',
                       'time_thread_start', 'reply_count', 'reply_users_count', 'reply_users', 'tm_thread_end']

            df = pd.DataFrame(data=data, columns=columns)
            df = df[df['sender_name'] != 'Not provided']
            dflist.append(df)

        if dflist:
            dfall = pd.concat(dflist, ignore_index=True)
            dfall['channel'] = path_channel.split('/')[-1].split('.')[0]
            dfall = dfall.reset_index(drop=True)
            return dfall
        else:
            return pd.DataFrame()

    def parse_slack_reaction(self, channel, path_channel):
        '''
        Get reactions.
        '''
        dfall_reaction = pd.DataFrame()
        combined = []

        for json_file in glob.glob(f"{path_channel}*.json"):
            with open(json_file, 'r') as slack_data:
                combined.append(slack_data)

        reaction_name, reaction_count, reaction_users, msg, user_id = [], [], [], [], []

        for k in combined:
            slack_data = json.load(open(k.name, 'r', encoding="utf-8"))

            for i_count, i in enumerate(slack_data):
                if 'reactions' in i.keys():
                    for j in range(len(i['reactions'])):
                        msg.append(i.get('text', ''))
                        user_id.append(i.get('user', ''))
                        reaction_name.append(i['reactions'][j].get('name', ''))
                        reaction_count.append(i['reactions'][j].get('count', ''))
                        reaction_users.append(",".join(i['reactions'][j].get('users', '')))

        data_reaction = zip(reaction_name, reaction_count, reaction_users, msg, user_id)
        columns_reaction = ['reaction_name', 'reaction_count', 'reaction_users_count', 'message', 'user_id']
        df_reaction = pd.DataFrame(data=data_reaction, columns=columns_reaction)
        df_reaction['channel'] = channel
        return df_reaction

    def get_community_participation(self):
        '''
        Get community participation.
        '''
        comm_dict = {}

        for json_file in glob.glob(f"{self.path}*.json"):
            with open(json_file, 'r') as slack_data:
                try:
                    a = json.load(slack_data)
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in file: {json_file}")
                    continue

                if not isinstance(a, list):
                    print(f"Invalid JSON format in file: {json_file}")
                    continue

                for msg in a:
                    if 'replies' in msg.keys():
                        for i in msg['replies']:
                            comm_dict[i['user']] = comm_dict.get(i['user'], 0) + 1

        return comm_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Slack history')
    parser.add_argument('--zip', help="Name of a zip file to import")
    args = parser.parse_args()
