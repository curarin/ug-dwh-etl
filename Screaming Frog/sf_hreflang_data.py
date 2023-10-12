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
bq_hreflang_missing = "####" #hreflang themen, wird appended
bq_hreflang_non200 = "####"

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
path_hreflang_unlinked_data = login_now("hreflang_missing_return_links.csv", "/tmp/hreflang_unlinked_data.csv")
path_hreflang_non200_data = login_now("hreflang_non200_hreflang_urls.csv", "/tmp/hreflang_non200_data.csv")

### Load Dataframes
df_hreflang_unlinked = pd.read_csv(path_hreflang_unlinked_data, low_memory=False)
df_hreflang_non200 = pd.read_csv(path_hreflang_non200_data, low_memory=False)
print("dataframes loaded")

# Column Renaming for GBQ
dataframes_for_renaming = [df_hreflang_unlinked,
                          df_hreflang_non200]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
print("renaming is done for gbq")

#crawldate
df_hreflang_unlinked["crawl_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
df_hreflang_non200["crawl_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")

### To BigQuery
#appending
pandas_gbq.to_gbq(df_hreflang_unlinked, bq_hreflang_missing, project_id="###", if_exists="replace", credentials=credentials)
print("df_hreflang_unlinked pushed to gbq")

pandas_gbq.to_gbq(df_hreflang_non200, bq_hreflang_non200, project_id="###", if_exists="replace", credentials=credentials)
print("df_hreflang_non200 pushed to gbq")



