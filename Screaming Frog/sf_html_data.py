def hello_world(message):
    message = "started"
    return message

print(hello_world("message"))
import pandas as pd
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import pandas_gbq
import re
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# Variablen für BigQuery
bq_content = "###" #für interne verlinkung der content - combi mit bq_inlinks
bq_html = "###"
bq_content_history = "###"

credentials = service_account.Credentials.from_service_account_file(
        'key.json',
    )

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

def login_now(wanted_file, tmp_path):
    """
    Google Drive service with a service account.
    note: for the service account to work, you need to share the folder or
    files with the service account email.

    :return: google auth
    """
    # Define the settings dict to use a service account
    # We also can use all options available for the settings dict like
    # oauth_scope,save_credentials,etc.
    settings = {
                "client_config_backend": "service",
                "service_config": {
                       "client_json_file_path":"key.json",
                }
            }
    # Create instance of GoogleAuth
    gauth = GoogleAuth(settings=settings)
    # Authenticate
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
    destination_file = tmp_path #"/tmp/internal_html.csv"
    source_file = wanted_file #"internal_html_piraten.csv"
    # List files in Google Drive - q ist hier der ScreamingFrog Folder
    file_list = drive.ListFile({'q': "'1xnwS1KCPfcJ31C1B3ftDWKSwSZel2rxd' in parents and trashed=false"}).GetList()

    # Find file to update
    for file1 in file_list:
        if file1['title'] == source_file:
            file_of_interest = file1
            
    
    file_of_interest.GetContentFile(destination_file)
    return destination_file

### Call Function to get ScreamingFrog Files
path_internal_html = login_now("internal_html.csv", "/tmp/internal_html.csv")

### Load Dataframes
df_html = pd.read_csv(path_internal_html, low_memory=False)
print("dataframes loaded")

## Fallback falls Forbidden
if any((df_html['Address'] == 'https://www.urlaubsguru.de/') & (df_html['Status Code'] != 200)):
    title_teams = "SEO Datawarehouse Crawling | Forbidden"
    content_teams = f"""
    <h2>Check Screaming Frog Settings</h2>
    <p>Screaming Frog ist aktuell nicht in der Lage zu crawlen - https://www.urlaubsguru.de/ enthält einen Status Code ungleich 200 (3XX/4XX/5XX).</p>
    <ul>
    <li><a href=###'>Link zur SEO DWH Dokumentation</a></li>
    </ul>
    """
    url_teams_push = "###"
    send_teams(url_teams_push, content_teams, title_teams)
print("frog crawl = ok")

#verzeichnisse
columns = ["root", "directory", "page_level1", "page_level2", "page_level3", "page_level4"]

data_frames = [df_html]

for df in data_frames:
    for column in columns:
        df[column] = df["Address"].apply(lambda x: x.split('/')[columns.index(column) + 2] if len(x.split('/')) > columns.index(column) + 2 else None)

print("directory tagging done")

#function for concatenating agent columns
def concatenate_agents(row):
    agent_values = [str(row[column]) for column in row.index if column.startswith('travelogic_agents')]
    # Filter out 'nan' values and join the remaining values
    agent_values = [value for value in agent_values if value != 'nan']
    return ', '.join(agent_values)

### Data Cleaning
regex_pattern_content = r'^content-.*' # für content extraction
internal_pattern = r"https?://www.urlaubsguru.de.*"
whitelabel_pattern = r"(?!www\.)[a-zA-Z0-9-]+.urlaubsguru.de.*"
picture_extensions = ["jpg", "jpeg", "png", "gif", "bmp", "svg", "webp", "ico", "tiff"]
html_columns_to_keep = r"Address$|Status Code$|Title 1$|Meta Description 1$|H1-1$|Meta Robots 1|Canonical Link Element 1|Size|Word Count|Sentence Count|Average Words Per Sentence|Flesch Reading Ease Score|Readability|Text Ratio|Crawl Depth|Link Score|Unique Inlinks|Unique Outlinks|ibe_integration 1|content|number_of_deals 1|URL Inspection API Status|URL Inspection API Error|Summary|Coverage|Last Crawl|Days Since Last Crawled|Crawled As|Crawl Allowed|Page Fetch|Indexing Allowed|User-Declared Canonical|Google-Selected Canonical|Mobile Usability|Rich Results|Rich Results Types|Rich Results Type Errors|Rich Results Type Warnings|crawl_time|root|directory|page_level.*|website_type|Redirect URL$|crawl_time|content$|travellogic 1|ibe_agent_id|travelogic_agent_names"
print("variables set for data cleaning")

filtered_columns = df_html.columns[df_html.columns.str.match(regex_pattern_content)]
combined_strings = df_html.filter(regex=regex_pattern_content).astype(str).sum(axis=1)
df_html['content'] = combined_strings
df_html.drop(filtered_columns, axis=1, inplace=True)
df_html['travelogic_agent_names'] = df_html.apply(concatenate_agents, axis=1)
df_html = df.drop(columns=[column for column in df_html.columns if column.startswith('travelogic_agents')])
print("content and agent columns merged")

# crawl zeit ohne uhrzeit
df_html["crawl_time"] = pd.to_datetime(df_html["Crawl Timestamp"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%Y-%m-%d")
df_html["Last Crawl"] = pd.to_datetime(df_html["Last Crawl"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%Y-%m-%d")
print("timestamp set")

# Dokumente
df_html["doctype"] = df_html["Address"].str.contains("|".join(picture_extensions), case=False, regex=True)
df_html["doctype"] = df_html["doctype"].map({True: "Picture", False: "HTML"})
df_html_images = df_html[(df_html["doctype"] == "Picture")]
df_html = df_html[(df_html["doctype"] == "HTML")]
df_html['website_type'] = df_html['Address'].apply(lambda url: 'Internal' if re.search(internal_pattern, url) else "Whitelabel" if re.search(whitelabel_pattern, url) else 'External')
df_html["Sentence Count"] = df_html["Sentence Count"].fillna(0, inplace=True)
df_html["Sentence Count"] = df_html["Sentence Count"].astype(float)
df_html["Average Words Per Sentence"] = df_html["Average Words Per Sentence"].fillna(0, inplace=True)
df_html["Average Words Per Sentence"] = df_html["Average Words Per Sentence"].astype(float)
df_html["Flesch Reading Ease Score"] = df_html["Flesch Reading Ease Score"].fillna(0, inplace=True)
df_html["Flesch Reading Ease Score"] = df_html["Flesch Reading Ease Score"].astype(float)
df_html["Text Ratio"] = df_html["Text Ratio"].fillna(0, inplace=True)
df_html["Text Ratio"] = df_html["Text Ratio"].astype(float)
df_html = df_html.filter(regex=html_columns_to_keep)
df_html_current_content = df_html
df_html = df_html.drop("content", axis=1)
print("df_html is done")

# Column Renaming for GBQ
dataframes_for_renaming = [df_html, 
                           df_html_current_content]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
        
df_html.rename(columns={"size_(bytes)": "page_size"}, inplace=True)
df_html_current_content.rename(columns={"size_(bytes)": "page_size"}, inplace=True)

print("renaming is done for gbq")
### To BigQuery
#appending
pandas_gbq.to_gbq(df_html, bq_html, project_id="###", if_exists="append", credentials=credentials)
print("df_html pushed to gbq")

pandas_gbq.to_gbq(df_html_current_content, bq_content_history, project_id="###", if_exists="append", credentials=credentials)
print("df_html_content pushed to history gbq table")

#replacing
pandas_gbq.to_gbq(df_html_current_content, bq_content, project_id="###", if_exists="replace", credentials=credentials)
print("df_html_content pushed to gbq")

