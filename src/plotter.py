import os
import nltk
import pandas as pd
import matplotlib.pyplot as plt
from nltk.corpus import stopwords
from wordcloud import WordCloud

# Download NLTK stopwords
nltk.download('stopwords')


def get_top_10_user(df, channel='Random'):
    """Get the top 10 users with the highest number of messages sent to any channel."""

def draw_avg_reply_count(df, channel='Random'):
    """Visualize the average number of reply counts per sender."""
    

def draw_avg_reply_users_count(df, channel='Random'):
    """Visualize the average number of reply user counts per sender."""
    

def draw_wordcloud(msg_content, week):    
    """Generate and display a WordCloud for the given message content."""
    
def draw_user_reaction(df, channel='General'):
    """Visualize users with the most reactions in the specified channel."""
    


plt.show()  
