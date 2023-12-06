import argparse
import datetime
import json
import glob
import os
import pandas as pd

class SlackDataLoader:
    def __init__(self, path):
        self.path = path
        self.df = pd.DataFrame()
        self.channels = self.get_channels()
        self.users = self.get_users()

    def get_users(self):
        with open(os.path.join(self.path, "users.json"), "r") as f:
            users = json.load(f)
        return users

    def get_channels(self):
        with open(os.path.join(self.path, "channels.json"), "r") as f:
            channels = json.load(f)
        return channels
    
    def convert_2_timestamp(self, column, data):
        """Convert from Unix time to readable timestamp."""
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
            return []

    def slack_parser(self, path_channel):
        combined = []
        for json_file in glob.glob(f"{path_channel}/all-community-building*/*.json"):
            with open(json_file, "r", encoding="utf8") as slack_data:
                combined.append(json.load(slack_data))

        dflist = []
        for slack_data in combined:
            (
                msg_type,
                msg_content,
                sender_id,
                time_msg,
                msg_dist,
                time_thread_st,
                reply_users,
                reply_count,
                reply_users_count,
                tm_thread_end,
            ) = ([], [], [], [], [], [], [], [], [], [])

            for row in slack_data:
                if "bot_id" in row.keys():
                    continue
                else:
                    msg_type.append(row["type"])
                    msg_content.append(row["text"])
                    if "user_profile" in row.keys():
                        sender_id.append(row["user_profile"]["real_name"])
                    else:
                        sender_id.append("Not provided")
                    time_msg.append(row["ts"])
                    if (
                        "blocks" in row.keys()
                        and len(row["blocks"][0]["elements"][0]["elements"]) != 0
                    ):
                        msg_dist.append(
                            row["blocks"][0]["elements"][0]["elements"][0]["type"]
                        )
                    else:
                        msg_dist.append("reshared")
                    if "thread_ts" in row.keys():
                        time_thread_st.append(row["thread_ts"])
                    else:
                        time_thread_st.append(0)
                    if "reply_users" in row.keys():
                        reply_users.append(",".join(row["reply_users"]))
                    else:
                        reply_users.append(0)
                    if "reply_count" in row.keys():
                        reply_count.append(row["reply_count"])
                        reply_users_count.append(row["reply_users_count"])
                        tm_thread_end.append(row["latest_reply"])
                    else:
                        reply_count.append(0)
                        reply_users_count.append(0)
                        tm_thread_end.append(0)
            
            data = zip(
                msg_type,
                msg_content,
                sender_id,
                time_msg,
                msg_dist,
                time_thread_st,
                reply_count,
                reply_users_count,
                reply_users,
                tm_thread_end,
            )
            columns = [
                "msg_type",
                "msg_content",
                "sender_name",
                "msg_sent_time",
                "msg_dist_type",
                "time_thread_start",
                "reply_count",
                "reply_users_count",
                "reply_users",
                "tm_thread_end",
            ]

            df = pd.DataFrame(data=data, columns=columns)
            df = df[df["sender_name"] != "Not provided"]
            df['msg_sent_time_timestamp'] = self.convert_2_timestamp('msg_sent_time', df)
            df['time_thread_start_timestamp'] = self.convert_2_timestamp('time_thread_start', df)
            dflist.append(df)

        dfall = pd.concat(dflist, ignore_index=True)
        dfall["channel"] = path_channel.split("/")[-1].split(".")[0]
        dfall = dfall.reset_index(drop=True)

        self.df = dfall
        return dfall
    def parse_slack_reaction(self, path, channel):
        """get reactions"""
        dfall_reaction = pd.DataFrame()
        combined = []
        for json_file in glob.glob(f"{path}*.json"):
            with open(json_file, "r") as slack_data:
                combined.append(slack_data)

        reaction_name, reaction_count, reaction_users, msg, user_id = [], [], [], [], []

        for k in combined:
            slack_data = json.load(open(k.name, "r", encoding="utf-8"))

            for i_count, i in enumerate(slack_data):
                if "reactions" in i.keys():
                    for j in range(len(i["reactions"])):
                        msg.append(i["text"])
                        user_id.append(i["user"])
                        reaction_name.append(i["reactions"][j]["name"])
                        reaction_count.append(i["reactions"][j]["count"])
                        reaction_users.append(",".join(i["reactions"][j]["users"]))

        data_reaction = zip(reaction_name, reaction_count, reaction_users, msg, user_id)
        columns_reaction = [
            "reaction_name",
            "reaction_count",
            "reaction_users_count",
            "message",
            "user_id",
        ]
        df_reaction = pd.DataFrame(data=data_reaction, columns=columns_reaction)
        df_reaction["channel"] = channel
        return df_reaction


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Slack history")
    parser.add_argument("--zip", help="Name of a zip file to import")
    args = parser.parse_args()


