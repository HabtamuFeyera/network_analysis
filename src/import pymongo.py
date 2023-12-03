import pymongo
import json

# Read the Slack JSON file
with open('slack_data.json', 'r') as file:
    slack_data = json.load(file)

# Connect to MongoDB (assuming it's running locally)
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["slack_database"]  # Create or use an existing database
collection = db["slack_messages"]  # Create or use a collection

# Insert Slack messages into the collection
collection.insert_many(slack_data)
