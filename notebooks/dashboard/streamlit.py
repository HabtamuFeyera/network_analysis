import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from textblob import TextBlob
from pymongo import MongoClient


st.title("Slack Data Analysis")
f1 = st.file_uploader(":file_folder: Upload your file", type=['csv', 'xlsx'])
col1, col2, col3, col4 = st.columns((4))


#connect it to our database

client = MongoClient("mongodb://localhost:27017/")

db = client['Tenx']
raw = db['Slack data']
cleaned = db['clean data']

#we have two datas, one is raw, and the other is cleaned holding the features
#raw Data
datas = raw.find()
data = pd.DataFrame(list(datas))
#cleand Data
datas2 = cleaned.find()
clean = pd.DataFrame(list(datas2))



#EDA analysis
st.header('Exploratory Data Analysis')
st.subheader('Data Summary')
st.write(data.describe())

reply_counts = data.groupby('sender_name')['reply_count'].sum().sort_values(ascending=False)

top_senders = pd.DataFrame({
    'Sender Name': reply_counts[:10].index,
    'Reply Count': reply_counts[:10].values
})
with col1:
    st.subheader('Top 10 users with the highest reply count')
    st.bar_chart(top_senders.set_index('Sender Name'))



user_msg_counts = data['sender_name'].value_counts()
user_msg_counts_df = pd.DataFrame({'sender_name': user_msg_counts[:10].index, 'msg_count': user_msg_counts[:10].values})

with col2:
    st.subheader('Top 10 users with the highest message count')
    st.scatter_chart(user_msg_counts_df.set_index('sender_name'), height=500, width=100, use_container_width=True)


# with col1:
#     st.subheader(" :gear: Top 10 Senders by Pie")
#     fig = px.pie(user_msg_counts_df, hole=0.5)
#     st.plotly_chart(fig)

    
user_message_counts = data.groupby('sender_name').size()

top_10_users = user_message_counts.nlargest(10)

user_message_counts_df = pd.DataFrame({'message_count': top_10_users.values}, index=top_10_users.index)
with col1:
   st.subheader("Big time message writers")
   st.bar_chart(user_message_counts_df, color=["#ffaa00"]) 



# Convert 'msg_sent_time' column to datetime format
data['msg_sent_time'] = pd.to_datetime(data['msg_sent_time'], infer_datetime_format=True)

# Extract hour from message timestamps
data['hour_sent'] = data['msg_sent_time'].dt.hour

# Group messages by hour and count the number of messages in each hour
hourly_message_count = data.groupby('hour_sent').size()


hour_peak = hourly_message_count.idxmax()
max_messages = hourly_message_count.max()

with col2:
    st.subheader('Hourly messages sent')
    st.line_chart(hourly_message_count, width=200, height=200)
    st.write(f"The peak hour with the highest number of messages is hour {hour_peak} with {max_messages} messages.")


# Sentiment Analysis
st.header('Sentiment Analysis')
st.subheader('Sentiment Distribution over all channels')
sentiment_scores = data['msg_content'].apply(lambda x: TextBlob(x).sentiment.polarity)
sns.histplot(sentiment_scores, kde=True)
st.pyplot()

# EDA
st.header('Exploratory Data Analysis')
st.subheader('Data Summary')
st.write(data.describe())

st.subheader('Data Visualization')
# Add your EDA visualizations here

# LDA
st.header('Latent Dirichlet Allocation')
st.subheader('Topic Modeling')
vectorizer = CountVectorizer(stop_words='english')
X = vectorizer.fit_transform(data['msg_content'])
lda = LatentDirichletAllocation(n_components=5, random_state=42)
lda.fit(X)
feature_names = vectorizer.get_feature_names_out()
for topic_idx, topic in enumerate(lda.components_):
    st.write(f"Topic #{topic_idx+1}")
    st.write([feature_names[i] for i in topic.argsort()[:-10 - 1:-1]])

