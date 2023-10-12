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
bq_inlinks = "###" #für inlink data für interne verlinkung optimierung
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
path_inlinks = login_now("all_inlinks.csv", "/tmp/inlinks.csv")

### Load Dataframes
df_inlinks = pd.read_csv(path_inlinks, low_memory=False)
print("dataframes loaded")


#verzeichnisse
df_inlinks["root"] = df_inlinks["Destination"].apply(lambda x: x.split('/')[2])
df_inlinks["directory"] = df_inlinks["Destination"].apply(lambda x: x.split('/')[3] if len(x.split('/')) > 3 else None)
df_inlinks["page_level1"] = df_inlinks["Destination"].apply(lambda x: x.split('/')[4] if len(x.split('/')) > 4 else None)
df_inlinks["page_level2"] = df_inlinks["Destination"].apply(lambda x: x.split('/')[5] if len(x.split('/')) > 5 else None)
df_inlinks["page_level3"] = df_inlinks["Destination"].apply(lambda x: x.split('/')[6] if len(x.split('/')) > 6 else None)
df_inlinks["page_level4"] = df_inlinks["Destination"].apply(lambda x: x.split('/')[7] if len(x.split('/')) > 7 else None)
print("directory tagging done")

### Data Cleaning
internal_pattern = r"https?://www.urlaubsguru.de.*"
whitelabel_pattern = r"(?!www\.)[a-zA-Z0-9-]+.urlaubsguru.de.*"
picture_extensions = ["jpg", "jpeg", "png", "gif", "bmp", "svg", "webp", "ico", "tiff"]
html_columns_to_keep = r"Address$|Status Code$|Title 1$|Meta Description 1$|H1-1$|Meta Robots 1|Canonical Link Element 1|Size|Word Count|Sentence Count|Average Words Per Sentence|Flesch Reading Ease Score|Readability|Text Ratio|Crawl Depth|Link Score|Unique Inlinks|Unique Outlinks|ibe_integration 1|content|number_of_deals 1|URL Inspection API Status|URL Inspection API Error|Summary|Converage|Last Crawl|Days Since Last Crawled|Crawled As|Crawl Allowed|Page Fetch|Indexing Allowed|User-Declared Canonical|Google-Selected Canonical|Mobile Usability|Rich Results|Rich Results Types|Rich Results Type Errors|Rich Results Type Warnings|crawl_time|root|directory|page_level.*|website_type|Redirect URL$|crawl_time|content$|Ahrefs"
print("variables set for data cleaning")


# Dokumente

#inlinks
df_inlinks["crawl_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
df_inlinks['destination_type'] = df_inlinks['Destination'].apply(lambda url: 'Internal' if re.search(internal_pattern, url) else "Whitelabel" if re.search(whitelabel_pattern, url) else 'External')
df_inlinks['source_type'] = df_inlinks['Source'].apply(lambda url: 'Internal' if re.search(internal_pattern, url) else "Whitelabel" if re.search(whitelabel_pattern, url) else 'External')
print("df_inlinks is done")

# Column Renaming for GBQ
dataframes_for_renaming = [df_inlinks]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
        
df_inlinks.rename(columns={"size_(bytes)": "page_size"}, inplace=True)
print("renaming is done for gbq")
### To BigQuery
#appending
pandas_gbq.to_gbq(df_inlinks, bq_inlinks, project_id="###", if_exists="replace", credentials=credentials)
print("df_inlinks pushed to gbq")


