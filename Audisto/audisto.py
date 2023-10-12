def hello_world(message):
    message = "started"
    return message

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime
import datetime
from datetime import date
import math
from io import StringIO
import pandas_gbq
from google.oauth2 import service_account
import time

# api settings
chunk_size = 100

### auth variables
username = "###"
pw = "###"

#api endpoint
url_for_crawl_list = "https://api.audisto.com/1.0/crawls/"


# wanted daterange
today = date.today() 

# Variablen für BigQuery
bq_audisto = "###"
credentials = service_account.Credentials.from_service_account_file(
        'key.json',
    )

### teams
def send_teams(webhook_url:str, content:str, title:str, color:str="000000") -> int:
    """
      - Send a teams notification to the desired webhook_url
      - Returns the status code of the HTTP request
        - webhook_url : the url you got from the teams webhook configuration
        - content : your formatted notification content
        - title : the message that'll be displayed as title, and on phone notifications
        - color (optional) : hexadecimal code of the notification's top line color, default corresponds to black
    """
    response = requests.post(
        url=webhook_url,
        headers={"Content-Type": "application/json"},
        json={
            "themeColor": color,
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "activitySubtitle": content
            }],
        },
    )
    return response.status_code # Should be 200

response = requests.get(url=url_for_crawl_list, auth=HTTPBasicAuth(username, pw))

if response.status_code == 200:
    data = response.json()
    item_list = []
    for item in data:
        crawl_id = item.get("id")
        crawl_timestamp = item.get("timestamps")
        crawl_time_started = crawl_timestamp["started"]
        item_list.append({"id": crawl_id, "crawl_time": crawl_time_started})    
else:
    # Error occurred
    title_teams = "SEO Datawarehouse Audisto | Issue raised"
    content_teams = f"""
    Audisto Crawl Error: 
    Die Crawl-History konnte nicht abgerufen werden - Status Code "{response.status_code}".
    """
    url_teams_push = "###"
    send_teams(url_teams_push, content_teams, title_teams)  
    

list_with_latest_crawls = pd.DataFrame(item_list)
list_with_latest_crawls["crawl_time"] = pd.to_datetime(list_with_latest_crawls["crawl_time"], format="%Y-%m-%dT%H:%M:%SZ").dt.date
latest_crawl_id = list_with_latest_crawls[list_with_latest_crawls["crawl_time"] == today]["id"].values[0]

### Extract needed total row count for number of chunks

url_for_pages_data = f"https://api.audisto.com/2.0/crawls/{latest_crawl_id}/pages"

params = {
    "filter": "status:2",
    "deep": 1,
    "chunk": 0,
    "chunksize": f"{chunk_size}"
}

response_latest_crawl = requests.get(url=url_for_pages_data, params=params, auth=HTTPBasicAuth(username, pw))

if response_latest_crawl.status_code == 200:
    data_total_rows = response_latest_crawl.json()
    nr_of_total_rows = data_total_rows["chunk"]["total"]    
else:
    # Error occurred
    print('Error for Response Latest Crawl:', response_latest_crawl.status_code)  

chunks_needed = math.ceil((nr_of_total_rows / chunk_size))

url_for_crawl_data = f"https://api.audisto.com/2.0/crawls/{latest_crawl_id}/pages"
csv_data = []


for x in range(chunks_needed):  
    params_crawl_data = {
        "filter": "status:2",
        "deep": 1,
        "chunk": x,
        "chunksize": chunk_size,
        "output": "csv"
    }
    
    try:
        response_crawl_data = requests.get(url=url_for_crawl_data, params=params_crawl_data, auth=HTTPBasicAuth(username, pw))
        response_crawl_data.raise_for_status()  # Raise an exception if the request was not successful
        
        csv_data.append(response_crawl_data.text)
        time.sleep(1)
    except requests.exceptions.RequestException as e:
        title_teams = "SEO Datawarehouse Audisto | Issue raised"
        content_teams = f"""
        Error occurred during request for chunk {x}: {e}
        """
        url_teams_push = "###"
        send_teams(url_teams_push, content_teams, title_teams)  
        continue  # Skip to the next iteration if an error occurs
    
merged_csv_data = "\n".join(csv_data)
df = pd.read_csv(StringIO(merged_csv_data))

### Work with DF
columns_to_keep = [
    "Url",
    "Page Rank",
    "Chei Rank",
]
df = df.filter(columns_to_keep)
df = df[df["Url"] != "Url"]
df["Page Rank"] = df["Page Rank"].astype(float)
df["Chei Rank"] = df["Chei Rank"].astype(float)

columns = ["root", "directory", "page_level1", "page_level2", "page_level3", "page_level4"]
data_frames = [df]

for df in data_frames:
    for column in columns:
        df[column] = df["Url"].apply(lambda x: x.split('/')[columns.index(column) + 2] if len(x.split('/')) > columns.index(column) + 2 else None)

print("directory tagging done")

df["crawl_date"] = today

# Column Renaming for GBQ
dataframes_for_renaming = [df]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
        
df.rename(columns={"url": "address"}, inplace=True)
pandas_gbq.to_gbq(df, bq_audisto, project_id="###", if_exists="append", credentials=credentials)
print("Audisto Data pushed to DWH")