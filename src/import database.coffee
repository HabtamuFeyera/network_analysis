import subprocess
import os

# Seting up my connection details
mongo_db = 'slack_database'  
mongo_uri = 'mongodb://localhost:27017/'

# Specify the path
path = 'C:\Users\ok\Desktop\Week_0\week-0' # Path of the directory containing JSON files

# Iterate through files in the directory

combined = []
    for json_file in glob.glob(f"{path_channel}/*.json"):
        with open(json_file, 'r', encoding="utf8") as slack_data:
            file = json.load(slack_data)
            combined.append(file)

for filename in combined:
    if filename.endswith('.json'):  # Process only JSON files
        collection_name = filename.split('.')[0]  # Use file name as collection name
        file_path = os.path.join(path, filename)
        
        # Run mongoimport command for each file
        command = f'mongoimport --db {mongo_db} --collection {collection_name} --file {file_path} --jsonArray'
        subprocess.run(command, shell=True)
