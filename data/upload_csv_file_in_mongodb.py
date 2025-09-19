#%%
# air1234 /airuser

#%% Connection
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
uri = "mongodb+srv://airuser:air1234@airflowtest.qnsdqx9.mongodb.net/?retryWrites=true&w=majority&appName=airflowtest"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
# %%
# load the data
import os
import sys
import pandas as pd

# Set working directory to where the script is located
script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
os.chdir(script_dir)

# Confirm the current working directory
print("Python working directory:", os.getcwd())

# Build full path to the CSV
csv_path = os.path.join(os.getcwd(), "airsample_dataset.csv")

# Read the CSV
df = pd.read_csv(csv_path)
print(df.head())
# %%
# deal with missing values
print(df.isnull().sum()) #   Gender: 5047

print("Total rows before dropping nulls:", df.shape[0]) # 50000

df_cleaned = df.dropna()

print("Total rows after dropping nulls:", len(df_cleaned)) #  44953
# %%
# data prepartion for insert
list_of_dict = df_cleaned.to_dict(orient='records')

list_of_dict
# %%
db = client['mydata']

#%% Create or access collection safely
collection_name = "customer"
if collection_name in db.list_collection_names():
    print(f"Collection '{collection_name}' already exists.")
    collection = db[collection_name]
else:
    collection = db.create_collection(collection_name)
    print(f"Collection '{collection_name}' created.")
# %%
# insert the data
collection.insert_many(list_of_dict)
# %%
# check the result
collection.count_documents({})
# %% To check List all existing collections
print("Collections in database:", db.list_collection_names())

# Count how many documents are in the customer collection
print("Total documents in 'customer':", db['customer'].count_documents({}))

# Preview the first few documents in the collection
for doc in db['customer'].find().limit(5):
    print(doc)
# %%
