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
bq_backlinks = "###"
credentials = service_account.Credentials.from_service_account_file(
        'key.json',
    )


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
path_backlinks = login_now("link_metrics_all.csv", "/tmp/backlinks.csv")
print("frog file loaded into cloud function")
### Load Dataframes
df_backlinks = pd.read_csv(path_backlinks, low_memory=False)
print("dataframes loaded")

columns = ["root", "directory", "page_level1", "page_level2", "page_level3", "page_level4"]
data_frames = [df_backlinks]
for df in data_frames:
    for column in columns:
        df[column] = df["Address"].apply(lambda x: x.split('/')[columns.index(column) + 2] if len(x.split('/')) > columns.index(column) + 2 else None)

print("directory tagging done")

### Data Cleaning
picture_extensions = ["jpg", "jpeg", "png", "gif", "bmp", "svg", "webp", "ico", "tiff"]
df_backlinks["doctype"] = df_backlinks["Address"].str.contains("|".join(picture_extensions), case=False, regex=True)
df_backlinks["doctype"] = df_backlinks["doctype"].map({True: "Picture", False: "HTML"})

dataframes_for_renaming = [df_backlinks]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
        
df_backlinks["crawl_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
pandas_gbq.to_gbq(df_backlinks, bq_backlinks, project_id="#####", if_exists="append", credentials=credentials)

